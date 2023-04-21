package hyperdx

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/beeker1121/goque"
	"github.com/ricochet2200/go-disk-usage/du"
	"go.uber.org/atomic"
)

const (
	maxSize             = 3 * 1024 * 1024 // 3 mb
	sendSleepingBackoff = time.Second * 2
	sendRetries         = 4
	respReadLimit       = int64(4096)

	defaultHost           = "https://in.hyperdx.io"
	defaultDrainDuration  = 5 * time.Second
	defaultDiskThreshold  = 70.0 // represent % of the disk
	defaultCheckDiskSpace = true

	httpError = -1
)

// HyperdxSender instance of the
type HyperdxSender struct {
	queue             *goque.Queue
	drainDuration     time.Duration
	buf               *bytes.Buffer
	draining          atomic.Bool
	mux               sync.Mutex
	token             string
	url               string
	debug             io.Writer
	diskThreshold     float32
	checkDiskSpace    bool
	fullDisk          bool
	checkDiskDuration time.Duration
	dir               string
	httpClient        *http.Client
	httpTransport     *http.Transport
}

// Sender Alias to HyperdxSender
type Sender HyperdxSender

// SenderOptionFunc options for logz
type SenderOptionFunc func(*HyperdxSender) error

// New creates a new Hyperdx sender with a token and options
func New(token string, options ...SenderOptionFunc) (*HyperdxSender, error) {
	l := &HyperdxSender{
		buf:               bytes.NewBuffer(make([]byte, maxSize)),
		drainDuration:     defaultDrainDuration,
		url:               fmt.Sprintf("%s/?hdx_token=%s&hdx_platform=go", defaultHost, token),
		token:             token,
		dir:               fmt.Sprintf("%s%s%s%s%d", os.TempDir(), string(os.PathSeparator), "hyperdx-buffer", string(os.PathSeparator), time.Now().UnixNano()),
		diskThreshold:     defaultDiskThreshold,
		checkDiskSpace:    defaultCheckDiskSpace,
		fullDisk:          false,
		checkDiskDuration: 5 * time.Second,
	}

	tlsConfig := &tls.Config{}
	transport := &http.Transport{
		TLSClientConfig: tlsConfig,
	}
	// in case server side is sleeping - wait 10s instead of waiting for him to wake up
	client := &http.Client{
		Transport: transport,
		Timeout:   time.Second * 10,
	}
	l.httpClient = client
	l.httpTransport = transport

	for _, option := range options {
		if err := option(l); err != nil {
			return nil, err
		}
	}

	q, err := goque.OpenQueue(l.dir)
	if err != nil {
		return nil, err
	}

	l.queue = q
	go l.start()
	go l.isEnoughDiskSpace()
	return l, nil
}

// SetTempDirectory Use this temporary dir
func SetTempDirectory(dir string) SenderOptionFunc {
	return func(l *HyperdxSender) error {
		l.dir = dir
		return nil
	}
}

// SetUrl set the url which maybe different from the defaultUrl
func SetUrl(url string) SenderOptionFunc {
	return func(l *HyperdxSender) error {
		l.url = fmt.Sprintf("%s/?hdx_token=%s&hdx_platform=go", url, l.token)
		l.debugLog("logsender.go: Setting url to %s\n", l.url)
		return nil
	}
}

// SetDebug mode and send logs to this writer
func SetDebug(debug io.Writer) SenderOptionFunc {
	return func(l *HyperdxSender) error {
		l.debug = debug
		return nil
	}
}

// SetDrainDuration to change the interval between drains
func SetDrainDuration(duration time.Duration) SenderOptionFunc {
	return func(l *HyperdxSender) error {
		l.drainDuration = duration
		return nil
	}
}

// SetDrainDiskThreshold to change the maximum used disk space
func SetCheckDiskSpace(check bool) SenderOptionFunc {
	return func(l *HyperdxSender) error {
		l.checkDiskSpace = check
		return nil
	}
}

// SetDrainDiskThreshold to change the maximum used disk space
func SetDrainDiskThreshold(th int) SenderOptionFunc {
	return func(l *HyperdxSender) error {
		l.diskThreshold = float32(th)
		return nil
	}
}

func (l *HyperdxSender) isEnoughDiskSpace() bool {
	for {
		<-time.After(l.checkDiskDuration)
		if l.checkDiskSpace {
			usage := du.NewDiskUsage(l.dir)
			if usage.Usage()*100 > l.diskThreshold {
				l.debugLog("Logz.io: Dropping logs, as FS used space on %s is %g percent,"+
					" and the drop threshold is %g percent\n",
					l.dir, usage.Usage()*100, l.diskThreshold)
				l.fullDisk = true
			} else {
				l.fullDisk = false
			}
		} else {
			l.fullDisk = false
		}
	}
}

// Send the payload to logz.io
func (l *HyperdxSender) Send(payload []byte) error {
	if !l.fullDisk {
		_, err := l.queue.Enqueue(payload)
		return err
	}
	return nil
}

func (l *HyperdxSender) start() {
	l.drainTimer()
}

// Stop will close the LevelDB queue and do a final drain
func (l *HyperdxSender) Stop() {
	defer l.queue.Close()
	l.Drain()

}

func (l *HyperdxSender) tryToSendLogs() int {
	resp, err := l.httpClient.Post(l.url, "text/plain", l.buf)
	if err != nil {
		l.debugLog("logsender.go: Error sending logs to %s %s\n", l.url, err)
		return httpError
	}

	defer resp.Body.Close()
	statusCode := resp.StatusCode

	_, err = io.Copy(ioutil.Discard, io.LimitReader(resp.Body, respReadLimit))
	if err != nil {
		l.debugLog("Error reading response body: %v", err)
	}
	return statusCode
}

func (l *HyperdxSender) drainTimer() {
	for {
		time.Sleep(l.drainDuration)
		l.Drain()
	}
}

func (l *HyperdxSender) shouldRetry(attempt int, statusCode int) bool {
	retry := true
	switch statusCode {
	case http.StatusBadRequest:
		retry = false
	case http.StatusUnauthorized:
		retry = false
	case http.StatusOK:
		retry = false
	}

	if !retry && statusCode != http.StatusOK {
		l.requeue()
	} else if retry && attempt == (sendRetries-1) {
		l.requeue()
	}
	return retry
}

// Drain - Send remaining logs
func (l *HyperdxSender) Drain() {
	if l.draining.Load() {
		l.debugLog("logsender.go: Already draining\n")
		return
	}
	l.mux.Lock()
	l.debugLog("logsender.go: draining queue\n")
	defer l.mux.Unlock()
	l.draining.Toggle()
	defer l.draining.Toggle()

	l.buf.Reset()
	bufSize := l.dequeueUpToMaxBatchSize()
	if bufSize > 0 {
		backOff := sendSleepingBackoff
		toBackOff := false
		for attempt := 0; attempt < sendRetries; attempt++ {
			if toBackOff {
				l.debugLog("logsender.go: failed to send logs, trying again in %v\n", backOff)
				time.Sleep(backOff)
				backOff *= 2
			}
			statusCode := l.tryToSendLogs()
			if l.shouldRetry(attempt, statusCode) {
				toBackOff = true
			} else {
				break
			}
		}
	}
}

func (l *HyperdxSender) dequeueUpToMaxBatchSize() int {
	var (
		bufSize int
		err     error
	)
	for bufSize < maxSize && err == nil {
		item, err := l.queue.Dequeue()
		if err != nil {
			l.debugLog("queue state: %s\n", err)
		}
		if item != nil {
			// NewLine is appended tp item.Value
			if len(item.Value)+bufSize+1 > maxSize {
				break
			}
			bufSize += len(item.Value)
			l.debugLog("logsender.go: Adding item %d with size %d (total buffSize: %d)\n",
				item.ID, len(item.Value), bufSize)
			_, err := l.buf.Write(append(item.Value, '\n'))
			if err != nil {
				l.errorLog("error writing to buffer %s", err)
			}
		} else {
			break
		}
	}
	return bufSize
}

// Sync drains the queue
func (l *HyperdxSender) Sync() error {
	l.Drain()
	return nil
}

func (l *HyperdxSender) requeue() {
	l.debugLog("logsender.go: Requeue %s", l.buf.String())
	err := l.Send(l.buf.Bytes())
	if err != nil {
		l.errorLog("could not requeue logs %s", err)
	}
}

func (l *HyperdxSender) debugLog(format string, a ...interface{}) {
	if l.debug != nil {
		fmt.Fprintf(l.debug, format, a...)
	}
}

func (l *HyperdxSender) errorLog(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format, a...)
}

func (l *HyperdxSender) Write(p []byte) (n int, err error) {
	return len(p), l.Send(p)
}

func (l *HyperdxSender) CloseIdleConnections() {
	l.httpTransport.CloseIdleConnections()
}
