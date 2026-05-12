package tokilake

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

// S3Config holds S3-compatible object storage configuration for the tokiame client.
type S3Config struct {
	Endpoint        string `json:"endpoint"`          // e.g. "https://s3.us-east-1.amazonaws.com" or MinIO URL
	BucketName      string `json:"bucket_name"`       // e.g. "comfyui-outputs"
	AccessKeyID     string `json:"access_key_id"`
	AccessKeySecret string `json:"access_key_secret"`
	Region          string `json:"region,omitempty"`          // defaults to "auto"
	PublicBaseURL   string `json:"public_base_url,omitempty"` // CDN/public URL prefix; if empty, uses endpoint
	PathPrefix      string `json:"path_prefix,omitempty"`     // e.g. "comfyui/" — prepended to object keys
	ExpirationDays  int    `json:"expiration_days,omitempty"` // 0 = no expiry
}

// S3Uploader is a lightweight S3-compatible uploader using AWS Signature V4.
// It requires no external SDK — only the Go standard library.
type S3Uploader struct {
	config S3Config
}

// NewS3Uploader creates a new S3 uploader from the given config.
func NewS3Uploader(config S3Config) *S3Uploader {
	if config.Region == "" {
		config.Region = "auto"
	}
	if config.PublicBaseURL == "" {
		config.PublicBaseURL = config.Endpoint
	}
	config.Endpoint = strings.TrimRight(config.Endpoint, "/")
	config.PublicBaseURL = strings.TrimRight(config.PublicBaseURL, "/")
	return &S3Uploader{config: config}
}

// IsConfigured returns true if the S3 uploader has valid configuration.
func (u *S3Uploader) IsConfigured() bool {
	return u != nil &&
		u.config.Endpoint != "" &&
		u.config.BucketName != "" &&
		u.config.AccessKeyID != "" &&
		u.config.AccessKeySecret != ""
}

// Upload uploads data to S3 and returns the public URL.
// The key is the object key (path within the bucket). A date prefix is auto-added.
func (u *S3Uploader) Upload(ctx context.Context, data []byte, key string, contentType string) (string, error) {
	now := time.Now().UTC()
	datePrefix := fmt.Sprintf("%d-%02d-%02d/", now.Year(), now.Month(), now.Day())
	if u.config.PathPrefix != "" {
		key = strings.TrimRight(u.config.PathPrefix, "/") + "/" + key
	}
	fullKey := datePrefix + key

	objectURL := fmt.Sprintf("%s/%s/%s", u.config.Endpoint, u.config.BucketName, fullKey)

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, objectURL, bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("create S3 request: %w", err)
	}

	if contentType == "" {
		contentType = "application/octet-stream"
	}
	req.Header.Set("Content-Type", contentType)

	// Sign the request using AWS Signature V4
	u.signV4(req, data, now)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("S3 upload request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("S3 upload failed (HTTP %d): %s", resp.StatusCode, string(body))
	}

	publicURL := fmt.Sprintf("%s/%s/%s", u.config.PublicBaseURL, u.config.BucketName, fullKey)
	return publicURL, nil
}

// signV4 signs an HTTP request using AWS Signature Version 4.
func (u *S3Uploader) signV4(req *http.Request, payload []byte, now time.Time) {
	dateStamp := now.Format("20060102")
	amzDate := now.Format("20060102T150405Z")
	service := "s3"

	// Payload hash
	payloadHash := sha256Hex(payload)
	req.Header.Set("x-amz-content-sha256", payloadHash)
	req.Header.Set("x-amz-date", amzDate)

	// Host header (required for signing)
	host := req.URL.Host
	req.Header.Set("Host", host)

	// Canonical request
	signedHeaders, canonicalHeaders := buildCanonicalHeaders(req)
	canonicalRequest := strings.Join([]string{
		req.Method,
		req.URL.Path,
		req.URL.RawQuery,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")

	// String to sign
	credentialScope := fmt.Sprintf("%s/%s/%s/aws4_request", dateStamp, u.config.Region, service)
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256",
		amzDate,
		credentialScope,
		sha256Hex([]byte(canonicalRequest)),
	}, "\n")

	// Signing key
	signingKey := deriveSigningKey(u.config.AccessKeySecret, dateStamp, u.config.Region, service)
	signature := hex.EncodeToString(hmacSHA256(signingKey, []byte(stringToSign)))

	// Authorization header
	auth := fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		u.config.AccessKeyID, credentialScope, signedHeaders, signature)
	req.Header.Set("Authorization", auth)
}

func buildCanonicalHeaders(req *http.Request) (signedHeaders string, canonicalHeaders string) {
	headerKeys := make([]string, 0)
	headerMap := make(map[string]string)

	for key := range req.Header {
		lk := strings.ToLower(key)
		if lk == "host" || lk == "content-type" || strings.HasPrefix(lk, "x-amz-") {
			headerKeys = append(headerKeys, lk)
			headerMap[lk] = strings.TrimSpace(req.Header.Get(key))
		}
	}
	sort.Strings(headerKeys)

	var headerLines []string
	for _, key := range headerKeys {
		headerLines = append(headerLines, key+":"+headerMap[key])
	}

	canonicalHeaders = strings.Join(headerLines, "\n") + "\n"
	signedHeaders = strings.Join(headerKeys, ";")
	return
}

func sha256Hex(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func hmacSHA256(key []byte, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func deriveSigningKey(secret, dateStamp, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secret), []byte(dateStamp))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))
	return kSigning
}
