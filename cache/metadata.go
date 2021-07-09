package cache

import (
	"time"

	"github.com/moby/buildkit/cache/metadata"
	"github.com/moby/buildkit/client"
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
const keyETag = "etag"
const keyChecksum = "http.checksum"
const keyModTime = "http.modtime"
const keyDeleted = "cache.deleted"
// BlobSize is the packed blob size as specified in the oci descriptor
const keyBlobSize = "cache.blobsize"

// TODO(sipsma) remove need for this to be public by creating a wrapper interface around searching the full metadata store
const KeyCacheDirIndex = "cache-dir:"

// Deprecated keys, only retained when needed for migrating from older versions of buildkit
const deprecatedKeyEqualMutable = "cache.equalMutable"

type cacheMetadata struct {
	si *metadata.StorageItem
}

func MetadataFromStorageItem(si *metadata.StorageItem) Metadata {
	return &cacheMetadata{si}
}

type Metadata interface {
	ID() string
	CommitMetadata() error

	GetDescription() string
	QueueDescription(string) error

	GetCreatedAt() time.Time
	QueueCreatedAt(time.Time) error

	GetContentHash() ([]byte, error)
	SetContentHash([]byte) error

	GetETag() string
	QueueETag(string) error

	GetHTTPModTime() string
	QueueHTTPModTime(string) error

	GetHTTPChecksum() digest.Digest
	QueueHTTPChecksum(url string, d digest.Digest) error

	HasCachePolicyDefault() bool
	QueueCachePolicyDefault() error
	HasCachePolicyRetain() bool
	QueueCachePolicyRetain() error

	GetLayerType() string
	QueueLayerType(string) error

	GetRecordType() client.UsageRecordType
	QueueRecordType(client.UsageRecordType) error

	QueueCacheDirIndex(string) error
	ClearCacheDirIndex(string) error
}

func (md *cacheMetadata) ID() string {
	return md.si.ID()
}

func (md *cacheMetadata) CommitMetadata() error {
	return md.si.Commit()
}

func (md *cacheMetadata) QueueCacheDirIndex(id string) error {
	v, err := metadata.NewValue(KeyCacheDirIndex+id)
	if err != nil {
		return errors.Wrap(err, "failed to create etag value")
	}
	v.Index = KeyCacheDirIndex+id
	md.si.Queue(func(b *bolt.Bucket) error {
		return md.si.SetValue(b, KeyCacheDirIndex+id, v)
	})
	return nil
}

func (md *cacheMetadata) ClearCacheDirIndex(id string) error {
	md.si.Queue(func(b *bolt.Bucket) error {
		if err := md.si.SetValue(b, KeyCacheDirIndex+id, nil); err != nil {
			return err
		}
		// force clearing index, see #1836 https://github.com/moby/buildkit/pull/1836
		return md.si.ClearIndex(b.Tx(), KeyCacheDirIndex+id)
	})
	return nil
}

func (md *cacheMetadata) HasCachePolicyDefault() bool {
	return md.getCachePolicy() == cachePolicyDefault
}

func (md *cacheMetadata) QueueCachePolicyDefault() error {
	return md.queueCachePolicy(cachePolicyDefault)
}

func (md *cacheMetadata) HasCachePolicyRetain() bool {
	return md.getCachePolicy() == cachePolicyRetain
}

func (md *cacheMetadata) QueueCachePolicyRetain() error {
	return md.queueCachePolicy(cachePolicyRetain)
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

func (md *cacheMetadata) QueueETag(s string) error {
	v, err := metadata.NewValue(s)
	if err != nil {
		return errors.Wrap(err, "failed to create etag value")
	}
	md.si.Queue(func(b *bolt.Bucket) error {
		return md.si.SetValue(b, keyETag, v)
	})
	return nil
}

func (md *cacheMetadata) GetETag() string {
	v := md.si.Get(keyETag)
	if v == nil {
		return ""
	}
	var etag string
	if err := v.Unmarshal(&etag); err != nil {
		return ""
	}
	return etag
}

func (md *cacheMetadata) QueueHTTPModTime(s string) error {
	v, err := metadata.NewValue(s)
	if err != nil {
		return errors.Wrap(err, "failed to create modtime value")
	}
	md.si.Queue(func(b *bolt.Bucket) error {
		return md.si.SetValue(b, keyModTime, v)
	})
	return nil
}

func (md *cacheMetadata) GetHTTPModTime() string {
	v := md.si.Get(keyModTime)
	if v == nil {
		return ""
	}
	var modTime string
	if err := v.Unmarshal(&modTime); err != nil {
		return ""
	}
	return modTime
}

func (md *cacheMetadata) QueueHTTPChecksum(url string, d digest.Digest) error {
	v, err := metadata.NewValue(d)
	if err != nil {
		return errors.Wrap(err, "failed to create checksum value")
	}
	v.Index = url
	md.si.Queue(func(b *bolt.Bucket) error {
		return md.si.SetValue(b, keyChecksum, v)
	})
	return nil
}

func (md *cacheMetadata) GetHTTPChecksum() digest.Digest {
	v := md.si.Get(keyChecksum)
	if v == nil {
		return ""
	}
	var dgstStr string
	if err := v.Unmarshal(&dgstStr); err != nil {
		return ""
	}
	dgst, err := digest.Parse(dgstStr)
	if err != nil {
		return ""
	}
	return dgst
}

func (md *cacheMetadata) queueDiffID(str digest.Digest) error {
	if str == "" {
		return nil
	}
	v, err := metadata.NewValue(str)
	if err != nil {
		return errors.Wrap(err, "failed to create diffID value")
	}
	md.si.Update(func(b *bolt.Bucket) error {
		return md.si.SetValue(b, keyDiffID, v)
	})
	return nil
}

func (md *cacheMetadata) getMediaType() string {
	v := md.si.Get(keyMediaType)
	if v == nil {
		return md.ID()
	}
	var str string
	if err := v.Unmarshal(&str); err != nil {
		return ""
	}
	return str
}

func (md *cacheMetadata) queueMediaType(str string) error {
	if str == "" {
		return nil
	}
	v, err := metadata.NewValue(str)
	if err != nil {
		return errors.Wrap(err, "failed to create mediaType value")
	}
	md.si.Queue(func(b *bolt.Bucket) error {
		return md.si.SetValue(b, keyMediaType, v)
	})
	return nil
}

func (md *cacheMetadata) getSnapshotID() string {
	v := md.si.Get(keySnapshot)
	if v == nil {
		return md.ID()
	}
	var str string
	if err := v.Unmarshal(&str); err != nil {
		return ""
	}
	return str
}

func (md *cacheMetadata) queueSnapshotID(str string) error {
	if str == "" {
		return nil
	}
	v, err := metadata.NewValue(str)
	if err != nil {
		return errors.Wrap(err, "failed to create snapshot ID value")
	}
	md.si.Queue(func(b *bolt.Bucket) error {
		return md.si.SetValue(b, keySnapshot, v)
	})
	return nil
}

func (md *cacheMetadata) getDiffID() digest.Digest {
	v := md.si.Get(keyDiffID)
	if v == nil {
		return ""
	}
	var str string
	if err := v.Unmarshal(&str); err != nil {
		return ""
	}
	return digest.Digest(str)
}

func (md *cacheMetadata) queueChainID(str digest.Digest) error {
	if str == "" {
		return nil
	}
	v, err := metadata.NewValue(str)
	if err != nil {
		return errors.Wrap(err, "failed to create chainID value")
	}
	v.Index = "chainid:" + string(str)
	md.si.Update(func(b *bolt.Bucket) error {
		return md.si.SetValue(b, keyChainID, v)
	})
	return nil
}

func (md *cacheMetadata) getBlobChainID() digest.Digest {
	v := md.si.Get(keyBlobChainID)
	if v == nil {
		return ""
	}
	var str string
	if err := v.Unmarshal(&str); err != nil {
		return ""
	}
	return digest.Digest(str)
}

func (md *cacheMetadata) queueBlobChainID(str digest.Digest) error {
	if str == "" {
		return nil
	}
	v, err := metadata.NewValue(str)
	if err != nil {
		return errors.Wrap(err, "failed to create chainID value")
	}
	v.Index = "blobchainid:" + string(str)
	md.si.Update(func(b *bolt.Bucket) error {
		return md.si.SetValue(b, keyBlobChainID, v)
	})
	return nil
}

func (md *cacheMetadata) getChainID() digest.Digest {
	v := md.si.Get(keyChainID)
	if v == nil {
		return ""
	}
	var str string
	if err := v.Unmarshal(&str); err != nil {
		return ""
	}
	return digest.Digest(str)
}

func (md *cacheMetadata) queueBlob(str digest.Digest) error {
	if str == "" {
		return nil
	}
	v, err := metadata.NewValue(str)
	if err != nil {
		return errors.Wrap(err, "failed to create blob value")
	}
	md.si.Update(func(b *bolt.Bucket) error {
		return md.si.SetValue(b, keyBlob, v)
	})
	return nil
}

func (md *cacheMetadata) getBlob() digest.Digest {
	v := md.si.Get(keyBlob)
	if v == nil {
		return ""
	}
	var str string
	if err := v.Unmarshal(&str); err != nil {
		return ""
	}
	return digest.Digest(str)
}

func (md *cacheMetadata) queueBlobOnly(b bool) error {
	v, err := metadata.NewValue(b)
	if err != nil {
		return errors.Wrap(err, "failed to create blobonly value")
	}
	md.si.Queue(func(b *bolt.Bucket) error {
		return md.si.SetValue(b, keyBlobOnly, v)
	})
	return nil
}

func (md *cacheMetadata) getBlobOnly() bool {
	v := md.si.Get(keyBlobOnly)
	if v == nil {
		return false
	}
	var blobOnly bool
	if err := v.Unmarshal(&blobOnly); err != nil {
		return false
	}
	return blobOnly
}

func (md *cacheMetadata) queueDeleted() error {
	v, err := metadata.NewValue(true)
	if err != nil {
		return errors.Wrap(err, "failed to create deleted value")
	}
	md.si.Queue(func(b *bolt.Bucket) error {
		return md.si.SetValue(b, keyDeleted, v)
	})
	return nil
}

func (md *cacheMetadata) getDeleted() bool {
	v := md.si.Get(keyDeleted)
	if v == nil {
		return false
	}
	var deleted bool
	if err := v.Unmarshal(&deleted); err != nil {
		return false
	}
	return deleted
}

func (md *cacheMetadata) queueParent(parent string) error {
	if parent == "" {
		return nil
	}
	v, err := metadata.NewValue(parent)
	if err != nil {
		return errors.Wrap(err, "failed to create parent value")
	}
	md.si.Update(func(b *bolt.Bucket) error {
		return md.si.SetValue(b, keyParent, v)
	})
	return nil
}

func (md *cacheMetadata) getParent() string {
	v := md.si.Get(keyParent)
	if v == nil {
		return ""
	}
	var parent string
	if err := v.Unmarshal(&parent); err != nil {
		return ""
	}
	return parent
}

func (md *cacheMetadata) queueSize(s int64) error {
	v, err := metadata.NewValue(s)
	if err != nil {
		return errors.Wrap(err, "failed to create size value")
	}
	md.si.Queue(func(b *bolt.Bucket) error {
		return md.si.SetValue(b, keySize, v)
	})
	return nil
}

func (md *cacheMetadata) getSize() int64 {
	v := md.si.Get(keySize)
	if v == nil {
		return sizeUnknown
	}
	var size int64
	if err := v.Unmarshal(&size); err != nil {
		return sizeUnknown
	}
	return size
}

func (md *cacheMetadata) appendImageRef(s string) error {
	return md.si.GetAndSetValue(keyImageRefs, func(v *metadata.Value) (*metadata.Value, error) {
		var imageRefs []string
		if v != nil {
			if err := v.Unmarshal(&imageRefs); err != nil {
				return nil, err
			}
		}
		for _, existing := range imageRefs {
			if existing == s {
				return nil, metadata.ErrSkipSetValue
			}
		}
		imageRefs = append(imageRefs, s)
		v, err := metadata.NewValue(imageRefs)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create imageRefs value")
		}
		return v, nil
	})
}

func (md *cacheMetadata) getImageRefs() []string {
	v := md.si.Get(keyImageRefs)
	if v == nil {
		return nil
	}
	var refs []string
	if err := v.Unmarshal(&refs); err != nil {
		return nil
	}
	return refs
}

func (md *cacheMetadata) queueBlobSize(s int64) error {
	v, err := metadata.NewValue(s)
	if err != nil {
		return errors.Wrap(err, "failed to create blobsize value")
	}
	md.si.Queue(func(b *bolt.Bucket) error {
		return md.si.SetValue(b, keyBlobSize, v)
	})
	return nil
}

func (md *cacheMetadata) getBlobSize() int64 {
	v := md.si.Get(keyBlobSize)
	if v == nil {
		return sizeUnknown
	}
	var size int64
	if err := v.Unmarshal(&size); err != nil {
		return sizeUnknown
	}
	return size
}

func (md *cacheMetadata) getEqualMutable() string {
	v := md.si.Get(deprecatedKeyEqualMutable)
	if v == nil {
		return ""
	}
	var str string
	if err := v.Unmarshal(&str); err != nil {
		return ""
	}
	return str
}

func (md *cacheMetadata) queueEqualMutable(s string) error {
	v, err := metadata.NewValue(s)
	if err != nil {
		return errors.Wrapf(err, "failed to create %s meta value", deprecatedKeyEqualMutable)
	}
	md.si.Queue(func(b *bolt.Bucket) error {
		return md.si.SetValue(b, deprecatedKeyEqualMutable, v)
	})
	return nil
}

func (md *cacheMetadata) clearEqualMutable() error {
	md.si.Queue(func(b *bolt.Bucket) error {
		return md.si.SetValue(b, deprecatedKeyEqualMutable, nil)
	})
	return nil
}

func (md *cacheMetadata) queueCachePolicy(p cachePolicy) error {
	v, err := metadata.NewValue(p)
	if err != nil {
		return errors.Wrap(err, "failed to create cachePolicy value")
	}
	md.si.Queue(func(b *bolt.Bucket) error {
		return md.si.SetValue(b, keyCachePolicy, v)
	})
	return nil
}

func (md *cacheMetadata) getCachePolicy() cachePolicy {
	v := md.si.Get(keyCachePolicy)
	if v == nil {
		return cachePolicyDefault
	}
	var p cachePolicy
	if err := v.Unmarshal(&p); err != nil {
		return cachePolicyDefault
	}
	return p
}

func (md *cacheMetadata) QueueDescription(descr string) error {
	v, err := metadata.NewValue(descr)
	if err != nil {
		return errors.Wrap(err, "failed to create description value")
	}
	md.si.Queue(func(b *bolt.Bucket) error {
		return md.si.SetValue(b, keyDescription, v)
	})
	return nil
}

func (md *cacheMetadata) GetDescription() string {
	v := md.si.Get(keyDescription)
	if v == nil {
		return ""
	}
	var str string
	if err := v.Unmarshal(&str); err != nil {
		return ""
	}
	return str
}

func (md *cacheMetadata) QueueCreatedAt(tm time.Time) error {
	v, err := metadata.NewValue(tm.UnixNano())
	if err != nil {
		return errors.Wrap(err, "failed to create createdAt value")
	}
	md.si.Queue(func(b *bolt.Bucket) error {
		return md.si.SetValue(b, keyCreatedAt, v)
	})
	return nil
}

func (md *cacheMetadata) GetCreatedAt() time.Time {
	v := md.si.Get(keyCreatedAt)
	if v == nil {
		return time.Time{}
	}
	var tm int64
	if err := v.Unmarshal(&tm); err != nil {
		return time.Time{}
	}
	return time.Unix(tm/1e9, tm%1e9)
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

func (md *cacheMetadata) GetLayerType() string {
	v := md.si.Get(keyLayerType)
	if v == nil {
		return ""
	}
	var str string
	if err := v.Unmarshal(&str); err != nil {
		return ""
	}
	return str
}

func (md *cacheMetadata) QueueLayerType(value string) error {
	v, err := metadata.NewValue(value)
	if err != nil {
		return errors.Wrap(err, "failed to create layertype value")
	}
	md.si.Queue(func(b *bolt.Bucket) error {
		return md.si.SetValue(b, keyLayerType, v)
	})
	return nil
}

func (md *cacheMetadata) GetRecordType() client.UsageRecordType {
	v := md.si.Get(keyRecordType)
	if v == nil {
		return ""
	}
	var str string
	if err := v.Unmarshal(&str); err != nil {
		return ""
	}
	return client.UsageRecordType(str)
}

func (md *cacheMetadata) QueueRecordType(value client.UsageRecordType) error {
	v, err := metadata.NewValue(value)
	if err != nil {
		return errors.Wrap(err, "failed to create recordtype value")
	}
	md.si.Queue(func(b *bolt.Bucket) error {
		return md.si.SetValue(b, keyRecordType, v)
	})
	return nil
}
