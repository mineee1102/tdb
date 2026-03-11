package db

import (
	"context"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"
)

// SSHConfig holds SSH tunnel configuration.
type SSHConfig struct {
	Host       string `yaml:"host"`
	Port       int    `yaml:"port"`
	User       string `yaml:"user"`
	Password   string `yaml:"password,omitempty"`
	PrivateKey string `yaml:"private_key,omitempty"` // path to private key file
	Passphrase string `yaml:"passphrase,omitempty"`  // passphrase for private key

	// Remote (DB) endpoint
	RemoteHost string `yaml:"remote_host"`
	RemotePort int    `yaml:"remote_port"`
}

// SSHTunnel manages an SSH tunnel for database connections.
type SSHTunnel struct {
	config   SSHConfig
	client   *ssh.Client
	listener net.Listener
	localAddr string

	mu      sync.Mutex
	closed  bool
	wg      sync.WaitGroup
}

// NewSSHTunnel creates a new SSH tunnel from the given config.
func NewSSHTunnel(config SSHConfig) *SSHTunnel {
	return &SSHTunnel{config: config}
}

// Start establishes the SSH connection and starts the tunnel.
// Returns the local address (host:port) to use for database connections.
func (t *SSHTunnel) Start(ctx context.Context) (string, error) {
	sshConfig, err := t.buildSSHConfig()
	if err != nil {
		return "", fmt.Errorf("failed to build SSH config: %w", err)
	}

	// SSH port defaults
	sshPort := t.config.Port
	if sshPort == 0 {
		sshPort = 22
	}

	// Derive timeout from context deadline; fall back to 30 seconds.
	timeout := 30 * time.Second
	if dl, ok := ctx.Deadline(); ok {
		timeout = time.Until(dl)
	}

	addr := net.JoinHostPort(t.config.Host, fmt.Sprintf("%d", sshPort))

	// Establish TCP connection with timeout so the ctx deadline is honoured.
	tcpConn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return "", fmt.Errorf("failed to dial SSH (TCP): %w", err)
	}

	// Upgrade to SSH over the already-connected TCP socket.
	sshConn, chans, reqs, err := ssh.NewClientConn(tcpConn, addr, sshConfig)
	if err != nil {
		tcpConn.Close()
		return "", fmt.Errorf("failed to establish SSH session: %w", err)
	}
	client := ssh.NewClient(sshConn, chans, reqs)
	t.client = client

	// Listen on a random local port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		client.Close()
		return "", fmt.Errorf("failed to listen on local port: %w", err)
	}
	t.listener = listener
	t.localAddr = listener.Addr().String()

	// Start forwarding connections
	t.wg.Add(1)
	go t.acceptLoop()

	return t.localAddr, nil
}

// LocalAddr returns the local address of the tunnel (available after Start).
func (t *SSHTunnel) LocalAddr() string {
	return t.localAddr
}

// Close shuts down the SSH tunnel.
func (t *SSHTunnel) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil
	}
	t.closed = true

	var firstErr error
	if t.listener != nil {
		if err := t.listener.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if t.client != nil {
		if err := t.client.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	t.wg.Wait()
	return firstErr
}

// acceptLoop accepts connections on the local listener and forwards them.
func (t *SSHTunnel) acceptLoop() {
	defer t.wg.Done()

	remoteAddr := net.JoinHostPort(t.config.RemoteHost, fmt.Sprintf("%d", t.config.RemotePort))

	for {
		localConn, err := t.listener.Accept()
		if err != nil {
			t.mu.Lock()
			closed := t.closed
			t.mu.Unlock()
			if closed {
				return
			}
			continue
		}

		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			t.forward(localConn, remoteAddr)
		}()
	}
}

// forward tunnels data between a local connection and the remote endpoint via SSH.
func (t *SSHTunnel) forward(localConn net.Conn, remoteAddr string) {
	remoteConn, err := t.client.Dial("tcp", remoteAddr)
	if err != nil {
		localConn.Close()
		return
	}

	// closeOnce ensures both connections are closed exactly once,
	// which unblocks the other goroutine's Read call and prevents
	// goroutine leaks.
	var closeOnce sync.Once
	closeBoth := func() {
		closeOnce.Do(func() {
			localConn.Close()
			remoteConn.Close()
		})
	}
	defer closeBoth()

	// Bidirectional copy
	done := make(chan struct{}, 2)

	go func() {
		defer closeBoth() // unblock the other direction
		buf := make([]byte, 32*1024)
		for {
			n, err := localConn.Read(buf)
			if n > 0 {
				if _, writeErr := remoteConn.Write(buf[:n]); writeErr != nil {
					break
				}
			}
			if err != nil {
				break
			}
		}
		done <- struct{}{}
	}()

	go func() {
		defer closeBoth() // unblock the other direction
		buf := make([]byte, 32*1024)
		for {
			n, err := remoteConn.Read(buf)
			if n > 0 {
				if _, writeErr := localConn.Write(buf[:n]); writeErr != nil {
					break
				}
			}
			if err != nil {
				break
			}
		}
		done <- struct{}{}
	}()

	// Wait for both directions to finish
	<-done
	<-done
}

// buildSSHConfig creates an ssh.ClientConfig from the tunnel configuration.
func (t *SSHTunnel) buildSSHConfig() (*ssh.ClientConfig, error) {
	var authMethods []ssh.AuthMethod

	// Private key authentication
	if t.config.PrivateKey != "" {
		keyBytes, err := os.ReadFile(t.config.PrivateKey)
		if err != nil {
			return nil, fmt.Errorf("failed to read private key %s: %w", t.config.PrivateKey, err)
		}

		var signer ssh.Signer
		if t.config.Passphrase != "" {
			signer, err = ssh.ParsePrivateKeyWithPassphrase(keyBytes, []byte(t.config.Passphrase))
		} else {
			signer, err = ssh.ParsePrivateKey(keyBytes)
		}
		if err != nil {
			return nil, fmt.Errorf("failed to parse private key: %w", err)
		}
		authMethods = append(authMethods, ssh.PublicKeys(signer))
	}

	// Password authentication
	if t.config.Password != "" {
		authMethods = append(authMethods, ssh.Password(t.config.Password))
	}

	if len(authMethods) == 0 {
		return nil, fmt.Errorf("no SSH authentication method configured (password or private key required)")
	}

	return &ssh.ClientConfig{
		User:            t.config.User,
		Auth:            authMethods,
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), //nolint:gosec // TUI tool, user-initiated connections. TODO: known_hosts ファイル (~/.ssh/known_hosts) によるホスト鍵検証を将来的に実装する
	}, nil
}
