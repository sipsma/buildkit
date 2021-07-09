package cache

import (
	"context"
	"time"

	"github.com/moby/buildkit/cache/metadata"
	"github.com/moby/buildkit/client"
	"github.com/moby/buildkit/util/bklog"
	"github.com/opencontainers/go-digest"
	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

const sizeUnknown int64 = -1
const keySize = "snapshot.size"
const keyCachePolicy = "cache.cachePolicy"
const keyDescription = "cache.description"
const keyCreatedAt = "cache.createdAt"
const keyLastUsedAt = "cache.lastUsedAt"
const keyUsageCount = "cache.usageCount"
const keyLayerType = "cache.layerType"
const keyRecordType = "cache.recordType"
const keyCommitted = "snapshot.committed"
const keyParent = "cache.parent"
const keyDiffID = "cache.diffID"
const keyChainID = "cache.chainID"
const keyBlobChainID = "cache.blobChainID"
const keyBlob = "cache.blob"
const keySnapshot = "cache.snapshot"
const keyBlobOnly = "cache.blobonly"
const keyMediaType = "cache.mediatype"
const keyImageRefs = "cache.imageRefs"
const keyContentHash = "buildkit.contenthash.v0"
const keyCacheContextID = "buildkit.cacheContextID.v0"
const keyETag = "etag"
const keyHTTPChecksum = "http.checksum"
const keyModTime = "http.modtime"
const keyDeleted = "cache.deleted"
const keyCacheDir = "cache-dir"
const keySharedKey = "local.sharedKey"
const keyBlobSize = "cache.blobsize" // the packed blob size as specified in the oci descriptor
const keyGitRemote = "git-remote"
const keyGitSnapshot = "git-snapshot"

// Deprecated keys, only retained when needed for migrating from older versions of buildkit
const deprecatedKeyEqualMutable = "cache.equalMutable"

// Indexes
const blobchainIndex = "blobchainid:"
const chainIndex = "chainid:"
const cacheDirIndex = keyCacheDir + ":"
const sharedKeyIndex = keySharedKey + ":"
const gitRemoteIndex = keyGitRemote + "::"
const gitSnapshotIndex = keyGitSnapshot + "::"

type MetadataStore interface {
	SearchCacheDir(context.Context, string) ([]Metadata, error)
	SearchSharedKey(context.Context, string) ([]Metadata, error)
	SearchGitRemote(context.Context, string) ([]Metadata, error)
	SearchGitSnapshot(context.Context, string) ([]Metadata, error)
	SearchHTTPURLDigest(context.Context, digest.Digest) ([]Metadata, error)
}

type Metadata interface {
	ID() string

	GetDescription() string
	SetDescription(string) error

	GetCreatedAt() time.Time
	SetCreatedAt(time.Time) error

	GetContentHash() ([]byte, error)
	SetContentHash([]byte) error
	GetCacheContextID() string
	SetCacheContextID(string) error

	GetETag() string
	SetETag(string) error

	GetHTTPModTime() string
	SetHTTPModTime(string) error

	GetHTTPChecksum() digest.Digest
	SetHTTPChecksum(urlDgst digest.Digest, d digest.Digest) error

	HasCachePolicyDefault() bool
	SetCachePolicyDefault() error
	HasCachePolicyRetain() bool
	SetCachePolicyRetain() error

	GetLayerType() string
	SetLayerType(string) error

	GetRecordType() client.UsageRecordType
	SetRecordType(client.UsageRecordType) error

	SetCacheDirIndex(string) error
	ClearCacheDirIndex(string) error

	GetSharedKey() string
	SetSharedKey(string) error

	GetGitRemote() string
	SetGitRemote(string) error

	GetGitSnapshot() string
	SetGitSnapshot(string) error
}

func (cm *cacheManager) SearchCacheDir(ctx context.Context, id string) ([]Metadata, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.search(ctx, cacheDirIndex+id)
}

func (cm *cacheManager) SearchSharedKey(ctx context.Context, key string) ([]Metadata, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.search(ctx, sharedKeyIndex+key)
}

func (cm *cacheManager) SearchGitRemote(ctx context.Context, remote string) ([]Metadata, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.search(ctx, gitRemoteIndex+remote)
}

func (cm *cacheManager) SearchGitSnapshot(ctx context.Context, key string) ([]Metadata, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.search(ctx, gitSnapshotIndex+key)
}

func (cm *cacheManager) SearchHTTPURLDigest(ctx context.Context, dgst digest.Digest) ([]Metadata, error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.search(ctx, dgst.String())
}

// callers must hold cm.mu lock
func (cm *cacheManager) getMetadata(id string) (*cacheMetadata, bool) {
	if rec, ok := cm.records[id]; ok {
		return rec.cacheMetadata, true
	}
	si, ok := cm.store.Get(id)
	md := &cacheMetadata{si}
	return md, ok
}

func (cm *cacheManager) searchBlobchain(ctx context.Context, id digest.Digest) ([]Metadata, error) {
	return cm.search(ctx, blobchainIndex+id.String())
}

func (cm *cacheManager) searchChain(ctx context.Context, id digest.Digest) ([]Metadata, error) {
	return cm.search(ctx, chainIndex+id.String())
}

func (cm *cacheManager) search(ctx context.Context, idx string) ([]Metadata, error) {
	if sis, err := cm.store.Search(idx); err != nil {
		return nil, err
	} else {
		var mds []Metadata
		for _, si := range sis {
			// calling getMetadata ensures we return the same storage item object that's cached in memory
			md, ok := cm.getMetadata(si.ID())
			if !ok {
				bklog.G(ctx).Warnf("missing metadata for storage item %q during search for %q", si.ID(), idx)
				continue
			}
			if md.getDeleted() {
				continue
			}
			mds = append(mds, md)
		}
		return mds, nil
	}
}

type cacheMetadata struct {
	si *metadata.StorageItem
}

func (md *cacheMetadata) ID() string {
	return md.si.ID()
}

func (md *cacheMetadata) copyValuesTo(other *cacheMetadata) error {
	return md.si.CopyValuesTo(other.si)
}

func (md *cacheMetadata) commitMetadata() error {
	return md.si.Commit()
}

func (md *cacheMetadata) GetDescription() string {
	return md.getString(keyDescription)
}

func (md *cacheMetadata) SetDescription(descr string) error {
	return md.setValue(keyDescription, descr, "")
}

func (md *cacheMetadata) queueDescription(descr string) error {
	return md.queueValue(keyDescription, descr, "")
}

func (md *cacheMetadata) queueCommitted(b bool) error {
	return md.queueValue(keyCommitted, b, "")
}

func (md *cacheMetadata) getCommitted() bool {
	return md.getBool(keyCommitted)
}

func (md *cacheMetadata) GetLayerType() string {
	return md.getString(keyLayerType)
}

func (md *cacheMetadata) SetLayerType(value string) error {
	return md.setValue(keyLayerType, value, "")
}

func (md *cacheMetadata) GetRecordType() client.UsageRecordType {
	return client.UsageRecordType(md.getString(keyRecordType))
}

func (md *cacheMetadata) SetRecordType(value client.UsageRecordType) error {
	return md.setValue(keyRecordType, value, "")
}

func (md *cacheMetadata) queueRecordType(value client.UsageRecordType) error {
	return md.queueValue(keyRecordType, value, "")
}

func (md *cacheMetadata) SetCreatedAt(tm time.Time) error {
	return md.setTime(keyCreatedAt, tm, "")
}

func (md *cacheMetadata) queueCreatedAt(tm time.Time) error {
	return md.queueTime(keyCreatedAt, tm, "")
}

func (md *cacheMetadata) GetCreatedAt() time.Time {
	return md.getTime(keyCreatedAt)
}

func (md *cacheMetadata) GetGitSnapshot() string {
	// TODO double check this is exactly how it was previously
	return md.getString(keyGitSnapshot)
}

func (md *cacheMetadata) SetGitSnapshot(key string) error {
	// TODO double check this is exactly how it was previously
	return md.setValue(keyGitSnapshot, key, gitSnapshotIndex+key)
}

func (md *cacheMetadata) GetGitRemote() string {
	// TODO double check this is exactly how it was previously
	return md.getString(keyGitRemote)
}

func (md *cacheMetadata) SetGitRemote(key string) error {
	// TODO double check this is exactly how it was previously
	return md.setValue(keyGitRemote, key, gitRemoteIndex+key)
}

func (md *cacheMetadata) GetSharedKey() string {
	// TODO double check this is exactly how it was previously
	return md.getString(keySharedKey)
}

func (md *cacheMetadata) SetSharedKey(key string) error {
	// TODO double check this is exactly how it was previously
	return md.setValue(keySharedKey, key, sharedKeyIndex+key)
}

func (md *cacheMetadata) SetCacheDirIndex(id string) error {
	// TODO double check this is exactly how it was previously
	return md.setValue(keyCacheDir, id, cacheDirIndex+id)
}

func (md *cacheMetadata) ClearCacheDirIndex(id string) error {
	// TODO double check this is exactly how it was previously
	return md.si.Update(func(b *bolt.Bucket) error {
		if err := md.si.SetValue(b, keyCacheDir, nil); err != nil {
			return err
		}
		// force clearing index, see #1836 https://github.com/moby/buildkit/pull/1836
		return md.si.ClearIndex(b.Tx(), cacheDirIndex+id)
	})
}

func (md *cacheMetadata) HasCachePolicyDefault() bool {
	return md.getCachePolicy() == cachePolicyDefault
}

func (md *cacheMetadata) SetCachePolicyDefault() error {
	return md.setCachePolicy(cachePolicyDefault)
}

func (md *cacheMetadata) HasCachePolicyRetain() bool {
	return md.getCachePolicy() == cachePolicyRetain
}

func (md *cacheMetadata) SetCachePolicyRetain() error {
	return md.setCachePolicy(cachePolicyRetain)
}

func (md *cacheMetadata) SetContentHash(dt []byte) error {
	return md.si.SetExternal(keyContentHash, dt)
}

func (md *cacheMetadata) GetContentHash() ([]byte, error) {
	dt, err := md.si.GetExternal(keyContentHash)
	if err != nil {
		return nil, err
	}
	return dt, nil
}

func (md *cacheMetadata) SetETag(s string) error {
	return md.setValue(keyETag, s, "")
}

func (md *cacheMetadata) GetETag() string {
	return md.getString(keyETag)
}

func (md *cacheMetadata) SetHTTPModTime(s string) error {
	return md.setValue(keyModTime, s, "")
}

func (md *cacheMetadata) GetHTTPModTime() string {
	return md.getString(keyModTime)
}

func (md *cacheMetadata) SetHTTPChecksum(urlDgst digest.Digest, d digest.Digest) error {
	return md.setValue(keyHTTPChecksum, d, urlDgst.String())
}

func (md *cacheMetadata) GetHTTPChecksum() digest.Digest {
	return digest.Digest(md.getString(keyHTTPChecksum))
}

func (md *cacheMetadata) SetCacheContextID(id string) error {
	return md.setValue(keyCacheContextID, id, "")
}

func (md *cacheMetadata) GetCacheContextID() string {
	return md.getString(keyCacheContextID)
}

func (md *cacheMetadata) queueDiffID(str digest.Digest) error {
	return md.queueValue(keyDiffID, str, "")
}

func (md *cacheMetadata) getMediaType() string {
	return md.getString(keyMediaType)
}

func (md *cacheMetadata) queueMediaType(str string) error {
	return md.queueValue(keyMediaType, str, "")
}

func (md *cacheMetadata) getSnapshotID() string {
	return md.getString(keySnapshot)
}

func (md *cacheMetadata) queueSnapshotID(str string) error {
	return md.queueValue(keySnapshot, str, "")
}

func (md *cacheMetadata) getDiffID() digest.Digest {
	return digest.Digest(md.getString(keyDiffID))
}

func (md *cacheMetadata) queueChainID(str digest.Digest) error {
	return md.queueValue(keyChainID, str, chainIndex+str.String())
}

func (md *cacheMetadata) getBlobChainID() digest.Digest {
	return digest.Digest(md.getString(keyBlobChainID))
}

func (md *cacheMetadata) queueBlobChainID(str digest.Digest) error {
	return md.queueValue(keyBlobChainID, str, blobchainIndex+str.String())
}

func (md *cacheMetadata) getChainID() digest.Digest {
	return digest.Digest(md.getString(keyChainID))
}

func (md *cacheMetadata) queueBlob(str digest.Digest) error {
	return md.queueValue(keyBlob, str, "")
}

func (md *cacheMetadata) getBlob() digest.Digest {
	return digest.Digest(md.getString(keyBlob))
}

func (md *cacheMetadata) queueBlobOnly(b bool) error {
	return md.queueValue(keyBlobOnly, b, "")
}

func (md *cacheMetadata) getBlobOnly() bool {
	return md.getBool(keyBlobOnly)
}

func (md *cacheMetadata) queueDeleted() error {
	return md.queueValue(keyDeleted, true, "")
}

func (md *cacheMetadata) getDeleted() bool {
	return md.getBool(keyDeleted)
}

func (md *cacheMetadata) queueParent(parent string) error {
	return md.queueValue(keyParent, parent, "")
}

func (md *cacheMetadata) getParent() string {
	return md.getString(keyParent)
}

func (md *cacheMetadata) queueSize(s int64) error {
	return md.queueValue(keySize, s, "")
}

func (md *cacheMetadata) getSize() int64 {
	if size, ok := md.getInt64(keySize); ok {
		return size
	}
	return sizeUnknown
}

func (md *cacheMetadata) appendImageRef(s string) error {
	return md.appendStringSlice(keyImageRefs, s)
}

func (md *cacheMetadata) getImageRefs() []string {
	return md.getStringSlice(keyImageRefs)
}

func (md *cacheMetadata) queueBlobSize(s int64) error {
	return md.queueValue(keyBlobSize, s, "")
}

func (md *cacheMetadata) getBlobSize() int64 {
	if size, ok := md.getInt64(keyBlobSize); ok {
		return size
	}
	return sizeUnknown
}

func (md *cacheMetadata) getEqualMutable() string {
	return md.getString(deprecatedKeyEqualMutable)
}

func (md *cacheMetadata) clearEqualMutable() error {
	md.si.Queue(func(b *bolt.Bucket) error {
		return md.si.SetValue(b, deprecatedKeyEqualMutable, nil)
	})
	return nil
}

func (md *cacheMetadata) setCachePolicy(p cachePolicy) error {
	return md.setValue(keyCachePolicy, p, "")
}

func (md *cacheMetadata) getCachePolicy() cachePolicy {
	if i, ok := md.getInt64(keyCachePolicy); ok {
		return cachePolicy(i)
	}
	return cachePolicyDefault
}

func (md *cacheMetadata) getLastUsed() (int, *time.Time) {
	v := md.si.Get(keyUsageCount)
	if v == nil {
		return 0, nil
	}
	var usageCount int
	if err := v.Unmarshal(&usageCount); err != nil {
		return 0, nil
	}
	v = md.si.Get(keyLastUsedAt)
	if v == nil {
		return usageCount, nil
	}
	var lastUsedTs int64
	if err := v.Unmarshal(&lastUsedTs); err != nil || lastUsedTs == 0 {
		return usageCount, nil
	}
	tm := time.Unix(lastUsedTs/1e9, lastUsedTs%1e9)
	return usageCount, &tm
}

func (md *cacheMetadata) queueLastUsed() error {
	count, _ := md.getLastUsed()
	count++

	v, err := metadata.NewValue(count)
	if err != nil {
		return errors.Wrap(err, "failed to create usageCount value")
	}
	v2, err := metadata.NewValue(time.Now().UnixNano())
	if err != nil {
		return errors.Wrap(err, "failed to create lastUsedAt value")
	}
	md.si.Queue(func(b *bolt.Bucket) error {
		if err := md.si.SetValue(b, keyUsageCount, v); err != nil {
			return err
		}
		return md.si.SetValue(b, keyLastUsedAt, v2)
	})
	return nil
}

func (md *cacheMetadata) queueValue(key string, value interface{}, index string) error {
	v, err := metadata.NewValue(value)
	if err != nil {
		return errors.Wrap(err, "failed to create value")
	}
	v.Index = index
	md.si.Queue(func(b *bolt.Bucket) error {
		return md.si.SetValue(b, key, v)
	})
	return nil
}

func (md *cacheMetadata) setValue(key string, value interface{}, index string) error {
	v, err := metadata.NewValue(value)
	if err != nil {
		return errors.Wrap(err, "failed to create value")
	}
	v.Index = index
	return md.si.Update(func(b *bolt.Bucket) error {
		return md.si.SetValue(b, key, v)
	})
}

func (md *cacheMetadata) getString(key string) string {
	v := md.si.Get(key)
	if v == nil {
		return ""
	}
	var str string
	if err := v.Unmarshal(&str); err != nil {
		return ""
	}
	return str
}

func (md *cacheMetadata) setTime(key string, value time.Time, index string) error {
	return md.setValue(key, value.UnixNano(), index)
}

func (md *cacheMetadata) queueTime(key string, value time.Time, index string) error {
	return md.queueValue(key, value.UnixNano(), index)
}

func (md *cacheMetadata) getTime(key string) time.Time {
	v := md.si.Get(key)
	if v == nil {
		return time.Time{}
	}
	var tm int64
	if err := v.Unmarshal(&tm); err != nil {
		return time.Time{}
	}
	return time.Unix(tm/1e9, tm%1e9)
}

func (md *cacheMetadata) getBool(key string) bool {
	v := md.si.Get(key)
	if v == nil {
		return false
	}
	var b bool
	if err := v.Unmarshal(&b); err != nil {
		return false
	}
	return b
}

func (md *cacheMetadata) getInt64(key string) (int64, bool) {
	v := md.si.Get(key)
	if v == nil {
		return 0, false
	}
	var i int64
	if err := v.Unmarshal(&i); err != nil {
		return 0, false
	}
	return i, true
}

func (md *cacheMetadata) appendStringSlice(key string, value string) error {
	return md.si.GetAndSetValue(key, func(v *metadata.Value) (*metadata.Value, error) {
		var slice []string
		if v != nil {
			if err := v.Unmarshal(&slice); err != nil {
				return nil, err
			}
		}
		for _, existing := range slice {
			if existing == value {
				return nil, metadata.ErrSkipSetValue
			}
		}
		slice = append(slice, value)
		v, err := metadata.NewValue(slice)
		if err != nil {
			return nil, err
		}
		return v, nil
	})
}

func (md *cacheMetadata) getStringSlice(key string) []string {
	v := md.si.Get(key)
	if v == nil {
		return nil
	}
	var s []string
	if err := v.Unmarshal(&s); err != nil {
		return nil
	}
	return s
}
