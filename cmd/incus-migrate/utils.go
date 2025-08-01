package main

import (
	"bufio"
	"context"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"golang.org/x/sys/unix"

	incus "github.com/lxc/incus/v6/client"
	"github.com/lxc/incus/v6/internal/linux"
	"github.com/lxc/incus/v6/internal/migration"
	"github.com/lxc/incus/v6/internal/ports"
	internalUtil "github.com/lxc/incus/v6/internal/util"
	"github.com/lxc/incus/v6/internal/version"
	"github.com/lxc/incus/v6/shared/api"
	localtls "github.com/lxc/incus/v6/shared/tls"
	"github.com/lxc/incus/v6/shared/ws"
)

// MigrationType represents the type of the migration.
type MigrationType string

// MigrationTypeContainer defines the migration type value for a container.
const MigrationTypeContainer = MigrationType("container")

// MigrationTypeVM defines the migration type value for a virtual-machine.
const MigrationTypeVM = MigrationType("virtual-machine")

// MigrationTypeVolumeFilesystem defines the migration type value for a custom volume of type filesystem.
const MigrationTypeVolumeFilesystem = MigrationType("volume-filesystem")

// MigrationTypeVolumeBlock defines the migration type value for a custom volume of type block.
const MigrationTypeVolumeBlock = MigrationType("volume-block")

func transferRootfs(ctx context.Context, op incus.Operation, rootfs string, rsyncArgs string, migrationType MigrationType) error {
	opAPI := op.Get()

	// Connect to the websockets
	wsControl, err := op.GetWebsocket(opAPI.Metadata[api.SecretNameControl].(string))
	if err != nil {
		return err
	}

	abort := func(err error) error {
		protoSendError(wsControl, err)
		return err
	}

	wsFs, err := op.GetWebsocket(opAPI.Metadata[api.SecretNameFilesystem].(string))
	if err != nil {
		return abort(err)
	}

	// Setup control struct
	var fs migration.MigrationFSType
	var rsyncHasFeature bool

	if migrationType == MigrationTypeVM || migrationType == MigrationTypeVolumeBlock {
		fs = migration.MigrationFSType_BLOCK_AND_RSYNC
		rsyncHasFeature = false
	} else {
		fs = migration.MigrationFSType_RSYNC
		rsyncHasFeature = true
	}

	offerHeader := migration.MigrationHeader{
		RsyncFeatures: &migration.RsyncFeatures{
			Xattrs:   &rsyncHasFeature,
			Delete:   &rsyncHasFeature,
			Compress: &rsyncHasFeature,
		},
		Fs: &fs,
	}

	if migrationType == MigrationTypeVM || migrationType == MigrationTypeVolumeBlock {
		sourcePath := filepath.Join(rootfs, "root.img")
		size, err := BlockDiskSizeBytes(sourcePath)
		if err != nil {
			return abort(err)
		}

		offerHeader.VolumeSize = &size
		rootfs = internalUtil.AddSlash(rootfs)
	}

	err = migration.ProtoSend(wsControl, &offerHeader)
	if err != nil {
		return abort(err)
	}

	var respHeader migration.MigrationHeader
	err = migration.ProtoRecv(wsControl, &respHeader)
	if err != nil {
		return abort(err)
	}

	rsyncFeaturesOffered := offerHeader.GetRsyncFeaturesSlice()
	rsyncFeaturesResponse := respHeader.GetRsyncFeaturesSlice()

	if !reflect.DeepEqual(rsyncFeaturesOffered, rsyncFeaturesResponse) {
		return abort(fmt.Errorf("Offered rsync features (%v) differ from those in the migration response (%v)", rsyncFeaturesOffered, rsyncFeaturesResponse))
	}

	// Send the filesystem
	if migrationType != MigrationTypeVolumeBlock {
		err = rsyncSend(ctx, wsFs, rootfs, rsyncArgs, migrationType)
		if err != nil {
			return abort(fmt.Errorf("Failed sending filesystem volume: %w", err))
		}
	}

	if migrationType == MigrationTypeVM || migrationType == MigrationTypeVolumeBlock {
		// Send block volume
		f, err := os.Open(filepath.Join(rootfs, "root.img"))
		if err != nil {
			return abort(err)
		}

		defer func() { _ = f.Close() }()

		conn := ws.NewWrapper(wsFs)

		go func() {
			<-ctx.Done()
			_ = conn.Close()
			_ = f.Close()
		}()

		_, err = io.Copy(conn, f)
		if err != nil {
			return abort(fmt.Errorf("Failed sending block volume: %w", err))
		}

		err = conn.Close()
		if err != nil {
			return abort(err)
		}
	}

	// Check the result
	msg := migration.MigrationControl{}
	err = migration.ProtoRecv(wsControl, &msg)
	if err != nil {
		_ = wsControl.Close()
		return err
	}

	if !msg.GetSuccess() {
		return errors.New(msg.GetMessage())
	}

	return nil
}

func (m *cmdMigrate) connectLocal() (incus.InstanceServer, error) {
	args := incus.ConnectionArgs{}
	args.UserAgent = fmt.Sprintf("LXC-MIGRATE %s", version.Version)

	return incus.ConnectIncusUnix("", &args)
}

func (m *cmdMigrate) connectTarget(uri string, certPath string, keyPath string, authType string, token string) (incus.InstanceServer, string, error) {
	args := incus.ConnectionArgs{
		AuthType: authType,
	}

	clientFingerprint := ""

	if authType == api.AuthenticationMethodTLS {
		var clientCrt []byte
		var clientKey []byte

		// Generate a new client certificate for this
		if certPath == "" || keyPath == "" {
			var err error

			clientCrt, clientKey, err = localtls.GenerateMemCert(true, false)
			if err != nil {
				return nil, "", err
			}

			clientFingerprint, err = localtls.CertFingerprintStr(string(clientCrt))
			if err != nil {
				return nil, "", err
			}

			// When using certificate add tokens, there's no need to show the temporary certificate.
			if token == "" {
				fmt.Printf("\nYour temporary certificate is:\n%s\n", string(clientCrt))
			}
		} else {
			var err error

			clientCrt, err = os.ReadFile(certPath)
			if err != nil {
				return nil, "", fmt.Errorf("Failed to read client certificate: %w", err)
			}

			clientKey, err = os.ReadFile(keyPath)
			if err != nil {
				return nil, "", fmt.Errorf("Failed to read client key: %w", err)
			}
		}

		args.TLSClientCert = string(clientCrt)
		args.TLSClientKey = string(clientKey)
	}

	// Attempt to connect using the system CA
	args.UserAgent = fmt.Sprintf("LXC-MIGRATE %s", version.Version)
	c, err := incus.ConnectIncus(uri, &args)

	var certificate *x509.Certificate
	if err != nil {
		// Failed to connect using the system CA, so retrieve the remote certificate
		certificate, err = localtls.GetRemoteCertificate(uri, args.UserAgent)
		if err != nil {
			return nil, "", err
		}
	}

	// Handle certificate prompt
	if certificate != nil {
		serverCrt := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Raw})
		args.TLSServerCert = string(serverCrt)

		// Setup a new connection, this time with the remote certificate
		c, err = incus.ConnectIncus(uri, &args)
		if err != nil {
			return nil, "", err
		}
	}

	// Get server information
	srv, _, err := c.GetServer()
	if err != nil {
		return nil, "", err
	}

	// Check if our cert is already trusted
	if srv.Auth == "trusted" {
		fmt.Printf("\nRemote server:\n  Hostname: %s\n  Version: %s\n\n", srv.Environment.ServerName, srv.Environment.ServerVersion)
		return c, "", nil
	}

	if authType == api.AuthenticationMethodTLS {
		if token != "" {
			req := api.CertificatesPost{
				TrustToken: token,
			}

			err = c.CreateCertificate(req)
			if err != nil {
				return nil, "", fmt.Errorf("Failed to create certificate: %w", err)
			}
		} else {
			fmt.Println("A temporary client certificate was generated, use `incus config trust add` on the target server.")
			fmt.Println("")

			fmt.Print("Press ENTER after the certificate was added to the remote server: ")
			_, err = bufio.NewReader(os.Stdin).ReadString('\n')
			if err != nil {
				return nil, "", err
			}
		}
	} else {
		c.RequireAuthenticated(true)
	}

	// Get full server information
	srv, _, err = c.GetServer()
	if err != nil {
		if clientFingerprint != "" {
			_ = c.DeleteCertificate(clientFingerprint)
		}

		return nil, "", err
	}

	if srv.Auth == "untrusted" {
		return nil, "", errors.New("Server doesn't trust us after authentication")
	}

	fmt.Printf("\nRemote server:\n  Hostname: %s\n  Version: %s\n\n", srv.Environment.ServerName, srv.Environment.ServerVersion)

	return c, clientFingerprint, nil
}

func setupSource(path string, mounts []string) error {
	prefix := "/"
	if len(mounts) > 0 {
		prefix = mounts[0]
	}

	// Mount everything
	for _, mount := range mounts {
		target := fmt.Sprintf("%s/%s", path, strings.TrimPrefix(mount, prefix))

		// Mount the path
		err := unix.Mount(mount, target, "none", unix.MS_BIND, "")
		if err != nil {
			return fmt.Errorf("Failed to mount %s: %w", mount, err)
		}

		// Make it read-only
		err = unix.Mount("", target, "none", unix.MS_BIND|unix.MS_RDONLY|unix.MS_REMOUNT, "")
		if err != nil {
			return fmt.Errorf("Failed to make %s read-only: %w", mount, err)
		}
	}

	return nil
}

func parseURL(URL string) (string, error) {
	uri, err := url.Parse(URL)
	if err != nil {
		return "", err
	}

	// Create a URL with scheme and hostname since it wasn't provided
	if uri.Scheme == "" && uri.Host == "" && uri.Path != "" {
		uri, err = url.Parse(fmt.Sprintf("https://%s", uri.Path))
		if err != nil {
			return "", err
		}
	}

	// If no port was provided, use default port
	if uri.Port() == "" {
		uri.Host = fmt.Sprintf("%s:%d", uri.Hostname(), ports.HTTPSDefaultPort)
	}

	return uri.String(), nil
}

// BlockDiskSizeBytes returns the size of a block disk (path can be either block device or raw file).
func BlockDiskSizeBytes(blockDiskPath string) (int64, error) {
	if linux.IsBlockdevPath(blockDiskPath) {
		// Attempt to open the device path.
		f, err := os.Open(blockDiskPath)
		if err != nil {
			return -1, err
		}

		defer func() { _ = f.Close() }()
		fd := int(f.Fd())

		// Retrieve the block device size.
		res, err := unix.IoctlGetInt(fd, unix.BLKGETSIZE64)
		if err != nil {
			return -1, err
		}

		return int64(res), nil
	}

	// Block device is assumed to be a raw file.
	fi, err := os.Lstat(blockDiskPath)
	if err != nil {
		return -1, err
	}

	return fi.Size(), nil
}
