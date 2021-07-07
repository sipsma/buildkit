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
const keyParent = "cache.parent"
const keyMergeParents = "cache.mergeParents"

// BlobSize is the packed blob size as specified in the oci descriptor
const keyBlobSize = "cache.blobsize"

// TODO(sipsma) remove need for this to be public by creating a wrapper interface around searching the full metadata store
const KeyCacheDirIndex = "cache-dir:"

// Deprecated keys, only retained when needed for migrating from older versions of buildkit
const deprecatedKeyEqualMutable = "cache.equalMutable"

type storageItem struct {
	*metadata.StorageItem
}

func MetadataFromStorageItem(si *metadata.StorageItem) Metadata {
	return &storageItem{si}
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

func (si *storageItem) CommitMetadata() error {
	return si.StorageItem.Commit()
}

func (si *storageItem) QueueCacheDirIndex(id string) error {
	v, err := metadata.NewValue(KeyCacheDirIndex+id)
	if err != nil {
		return errors.Wrap(err, "failed to create etag value")
	}
	v.Index = KeyCacheDirIndex+id
	si.Queue(func(b *bolt.Bucket) error {
		return si.SetValue(b, KeyCacheDirIndex+id, v)
	})
	return nil
}

func (si *storageItem) ClearCacheDirIndex(id string) error {
	si.Queue(func(b *bolt.Bucket) error {
		if err := si.SetValue(b, KeyCacheDirIndex+id, nil); err != nil {
			return err
		}
		// force clearing index, see #1836 https://github.com/moby/buildkit/pull/1836
		return si.ClearIndex(b.Tx(), KeyCacheDirIndex+id)
	})
	return nil
}

func (si *storageItem) HasCachePolicyDefault() bool {
	return si.getCachePolicy() == cachePolicyDefault
}

func (si *storageItem) QueueCachePolicyDefault() error {
	return si.queueCachePolicy(cachePolicyDefault)
}

func (si *storageItem) HasCachePolicyRetain() bool {
	return si.getCachePolicy() == cachePolicyRetain
}

func (si *storageItem) QueueCachePolicyRetain() error {
	return si.queueCachePolicy(cachePolicyRetain)
}

func (si *storageItem) SetContentHash(dt []byte) error {
	return si.SetExternal(keyContentHash, dt)
}

func (si *storageItem) GetContentHash() ([]byte, error) {
	dt, err := si.GetExternal(keyContentHash)
	if err != nil {
		return nil, err
	}
	return dt, nil
}

func (si *storageItem) QueueETag(s string) error {
	v, err := metadata.NewValue(s)
	if err != nil {
		return errors.Wrap(err, "failed to create etag value")
	}
	si.Queue(func(b *bolt.Bucket) error {
		return si.SetValue(b, keyETag, v)
	})
	return nil
}

func (si *storageItem) GetETag() string {
	v := si.Get(keyETag)
	if v == nil {
		return ""
	}
	var etag string
	if err := v.Unmarshal(&etag); err != nil {
		return ""
	}
	return etag
}

func (si *storageItem) QueueHTTPModTime(s string) error {
	v, err := metadata.NewValue(s)
	if err != nil {
		return errors.Wrap(err, "failed to create modtime value")
	}
	si.Queue(func(b *bolt.Bucket) error {
		return si.SetValue(b, keyModTime, v)
	})
	return nil
}

func (si *storageItem) GetHTTPModTime() string {
	v := si.Get(keyModTime)
	if v == nil {
		return ""
	}
	var modTime string
	if err := v.Unmarshal(&modTime); err != nil {
		return ""
	}
	return modTime
}

func (si *storageItem) QueueHTTPChecksum(url string, d digest.Digest) error {
	v, err := metadata.NewValue(d)
	if err != nil {
		return errors.Wrap(err, "failed to create checksum value")
	}
	v.Index = url
	si.Queue(func(b *bolt.Bucket) error {
		return si.SetValue(b, keyChecksum, v)
	})
	return nil
}

func (si *storageItem) GetHTTPChecksum() digest.Digest {
	v := si.Get(keyChecksum)
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

func (si *storageItem) queueDiffID(str digest.Digest) error {
	if str == "" {
		return nil
	}
	v, err := metadata.NewValue(str)
	if err != nil {
		return errors.Wrap(err, "failed to create diffID value")
	}
	si.Update(func(b *bolt.Bucket) error {
		return si.SetValue(b, keyDiffID, v)
	})
	return nil
}

func (si *storageItem) getMediaType() string {
	v := si.Get(keyMediaType)
	if v == nil {
		return si.ID()
	}
	var str string
	if err := v.Unmarshal(&str); err != nil {
		return ""
	}
	return str
}

func (si *storageItem) queueMediaType(str string) error {
	if str == "" {
		return nil
	}
	v, err := metadata.NewValue(str)
	if err != nil {
		return errors.Wrap(err, "failed to create mediaType value")
	}
	si.Queue(func(b *bolt.Bucket) error {
		return si.SetValue(b, keyMediaType, v)
	})
	return nil
}

func (si *storageItem) getSnapshotID() string {
	v := si.Get(keySnapshot)
	if v == nil {
		return si.ID()
	}
	var str string
	if err := v.Unmarshal(&str); err != nil {
		return ""
	}
	return str
}

func (si *storageItem) queueSnapshotID(str string) error {
	if str == "" {
		return nil
	}
	v, err := metadata.NewValue(str)
	if err != nil {
		return errors.Wrap(err, "failed to create snapshot ID value")
	}
	si.Queue(func(b *bolt.Bucket) error {
		return si.SetValue(b, keySnapshot, v)
	})
	return nil
}

func (si *storageItem) getDiffID() digest.Digest {
	v := si.Get(keyDiffID)
	if v == nil {
		return ""
	}
	var str string
	if err := v.Unmarshal(&str); err != nil {
		return ""
	}
	return digest.Digest(str)
}

func (si *storageItem) queueChainID(str digest.Digest) error {
	if str == "" {
		return nil
	}
	v, err := metadata.NewValue(str)
	if err != nil {
		return errors.Wrap(err, "failed to create chainID value")
	}
	v.Index = "chainid:" + string(str)
	si.Update(func(b *bolt.Bucket) error {
		return si.SetValue(b, keyChainID, v)
	})
	return nil
}

func (si *storageItem) getBlobChainID() digest.Digest {
	v := si.Get(keyBlobChainID)
	if v == nil {
		return ""
	}
	var str string
	if err := v.Unmarshal(&str); err != nil {
		return ""
	}
	return digest.Digest(str)
}

func (si *storageItem) queueBlobChainID(str digest.Digest) error {
	if str == "" {
		return nil
	}
	v, err := metadata.NewValue(str)
	if err != nil {
		return errors.Wrap(err, "failed to create chainID value")
	}
	v.Index = "blobchainid:" + string(str)
	si.Update(func(b *bolt.Bucket) error {
		return si.SetValue(b, keyBlobChainID, v)
	})
	return nil
}

func (si *storageItem) getChainID() digest.Digest {
	v := si.Get(keyChainID)
	if v == nil {
		return ""
	}
	var str string
	if err := v.Unmarshal(&str); err != nil {
		return ""
	}
	return digest.Digest(str)
}

func (si *storageItem) queueBlob(str digest.Digest) error {
	if str == "" {
		return nil
	}
	v, err := metadata.NewValue(str)
	if err != nil {
		return errors.Wrap(err, "failed to create blob value")
	}
	si.Update(func(b *bolt.Bucket) error {
		return si.SetValue(b, keyBlob, v)
	})
	return nil
}

func (si *storageItem) getBlob() digest.Digest {
	v := si.Get(keyBlob)
	if v == nil {
		return ""
	}
	var str string
	if err := v.Unmarshal(&str); err != nil {
		return ""
	}
	return digest.Digest(str)
}

func (si *storageItem) queueBlobOnly(b bool) error {
	v, err := metadata.NewValue(b)
	if err != nil {
		return errors.Wrap(err, "failed to create blobonly value")
	}
	si.Queue(func(b *bolt.Bucket) error {
		return si.SetValue(b, keyBlobOnly, v)
	})
	return nil
}

func (si *storageItem) getBlobOnly() bool {
	v := si.Get(keyBlobOnly)
	if v == nil {
		return false
	}
	var blobOnly bool
	if err := v.Unmarshal(&blobOnly); err != nil {
		return false
	}
	return blobOnly
}

func (si *storageItem) queueDeleted() error {
	v, err := metadata.NewValue(true)
	if err != nil {
		return errors.Wrap(err, "failed to create deleted value")
	}
	si.Queue(func(b *bolt.Bucket) error {
		return si.SetValue(b, keyDeleted, v)
	})
	return nil
}

func (si *storageItem) getDeleted() bool {
	v := si.Get(keyDeleted)
	if v == nil {
		return false
	}
	var deleted bool
	if err := v.Unmarshal(&deleted); err != nil {
		return false
	}
	return deleted
}

func (si *storageItem) queueParent(parent string) error {
	if parent == "" {
		return nil
	}
	v, err := metadata.NewValue(parent)
	if err != nil {
		return errors.Wrap(err, "failed to create parent value")
	}
	si.Update(func(b *bolt.Bucket) error {
		return si.SetValue(b, keyParent, v)
	})
	return nil
}

func (si *storageItem) getParent() string {
	v := si.Get(keyParent)
	if v == nil {
		return ""
	}
	var parent string
	if err := v.Unmarshal(&parent); err != nil {
		return ""
	}
	return parent
}

func (si *storageItem) queueSize(s int64) error {
	v, err := metadata.NewValue(s)
	if err != nil {
		return errors.Wrap(err, "failed to create size value")
	}
	si.Queue(func(b *bolt.Bucket) error {
		return si.SetValue(b, keySize, v)
	})
	return nil
}

func (si *storageItem) getSize() int64 {
	v := si.Get(keySize)
	if v == nil {
		return sizeUnknown
	}
	var size int64
	if err := v.Unmarshal(&size); err != nil {
		return sizeUnknown
	}
	return size
}

func (si *storageItem) appendImageRef(s string) error {
	return si.GetAndSetValue(keyImageRefs, func(v *metadata.Value) (*metadata.Value, error) {
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

func (si *storageItem) getImageRefs() []string {
	v := si.Get(keyImageRefs)
	if v == nil {
		return nil
	}
	var refs []string
	if err := v.Unmarshal(&refs); err != nil {
		return nil
	}
	return refs
}

func (si *storageItem) queueBlobSize(s int64) error {
	v, err := metadata.NewValue(s)
	if err != nil {
		return errors.Wrap(err, "failed to create blobsize value")
	}
	si.Queue(func(b *bolt.Bucket) error {
		return si.SetValue(b, keyBlobSize, v)
	})
	return nil
}

func (si *storageItem) getBlobSize() int64 {
	v := si.Get(keyBlobSize)
	if v == nil {
		return sizeUnknown
	}
	var size int64
	if err := v.Unmarshal(&size); err != nil {
		return sizeUnknown
	}
	return size
}

func (si *storageItem) getEqualMutable() string {
	v := si.Get(deprecatedKeyEqualMutable)
	if v == nil {
		return ""
	}
	var str string
	if err := v.Unmarshal(&str); err != nil {
		return ""
	}
	return str
}

func (si *storageItem) queueEqualMutable(s string) error {
	v, err := metadata.NewValue(s)
	if err != nil {
		return errors.Wrapf(err, "failed to create %s meta value", deprecatedKeyEqualMutable)
	}
	si.Queue(func(b *bolt.Bucket) error {
		return si.SetValue(b, deprecatedKeyEqualMutable, v)
	})
	return nil
}

func (si *storageItem) clearEqualMutable() error {
	si.Queue(func(b *bolt.Bucket) error {
		return si.SetValue(b, deprecatedKeyEqualMutable, nil)
	})
	return nil
}

func (si *storageItem) queueCachePolicy(p cachePolicy) error {
	v, err := metadata.NewValue(p)
	if err != nil {
		return errors.Wrap(err, "failed to create cachePolicy value")
	}
	si.Queue(func(b *bolt.Bucket) error {
		return si.SetValue(b, keyCachePolicy, v)
	})
	return nil
}

func (si *storageItem) getCachePolicy() cachePolicy {
	v := si.Get(keyCachePolicy)
	if v == nil {
		return cachePolicyDefault
	}
	var p cachePolicy
	if err := v.Unmarshal(&p); err != nil {
		return cachePolicyDefault
	}
	return p
}

func (si *storageItem) QueueDescription(descr string) error {
	v, err := metadata.NewValue(descr)
	if err != nil {
		return errors.Wrap(err, "failed to create description value")
	}
	si.Queue(func(b *bolt.Bucket) error {
		return si.SetValue(b, keyDescription, v)
	})
	return nil
}

func (si *storageItem) GetDescription() string {
	v := si.Get(keyDescription)
	if v == nil {
		return ""
	}
	var str string
	if err := v.Unmarshal(&str); err != nil {
		return ""
	}
	return str
}

func (si *storageItem) QueueCreatedAt(tm time.Time) error {
	v, err := metadata.NewValue(tm.UnixNano())
	if err != nil {
		return errors.Wrap(err, "failed to create createdAt value")
	}
	si.Queue(func(b *bolt.Bucket) error {
		return si.SetValue(b, keyCreatedAt, v)
	})
	return nil
}

func (si *storageItem) GetCreatedAt() time.Time {
	v := si.Get(keyCreatedAt)
	if v == nil {
		return time.Time{}
	}
	var tm int64
	if err := v.Unmarshal(&tm); err != nil {
		return time.Time{}
	}
	return time.Unix(tm/1e9, tm%1e9)
}

func (si *storageItem) getLastUsed() (int, *time.Time) {
	v := si.Get(keyUsageCount)
	if v == nil {
		return 0, nil
	}
	var usageCount int
	if err := v.Unmarshal(&usageCount); err != nil {
		return 0, nil
	}
	v = si.Get(keyLastUsedAt)
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

func (si *storageItem) queueLastUsed() error {
	count, _ := si.getLastUsed()
	count++

	v, err := metadata.NewValue(count)
	if err != nil {
		return errors.Wrap(err, "failed to create usageCount value")
	}
	v2, err := metadata.NewValue(time.Now().UnixNano())
	if err != nil {
		return errors.Wrap(err, "failed to create lastUsedAt value")
	}
	si.Queue(func(b *bolt.Bucket) error {
		if err := si.SetValue(b, keyUsageCount, v); err != nil {
			return err
		}
		return si.SetValue(b, keyLastUsedAt, v2)
	})
	return nil
}

func (si *storageItem) GetLayerType() string {
	v := si.Get(keyLayerType)
	if v == nil {
		return ""
	}
	var str string
	if err := v.Unmarshal(&str); err != nil {
		return ""
	}
	return str
}

func (si *storageItem) QueueLayerType(value string) error {
	v, err := metadata.NewValue(value)
	if err != nil {
		return errors.Wrap(err, "failed to create layertype value")
	}
	si.Queue(func(b *bolt.Bucket) error {
		return si.SetValue(b, keyLayerType, v)
	})
	return nil
}

func (si *storageItem) GetRecordType() client.UsageRecordType {
	v := si.Get(keyRecordType)
	if v == nil {
		return ""
	}
	var str string
	if err := v.Unmarshal(&str); err != nil {
		return ""
	}
	return client.UsageRecordType(str)
}

func (si *storageItem) QueueRecordType(value client.UsageRecordType) error {
	v, err := metadata.NewValue(value)
	if err != nil {
		return errors.Wrap(err, "failed to create recordtype value")
	}
	si.Queue(func(b *bolt.Bucket) error {
		return si.SetValue(b, keyRecordType, v)
	})
	return nil
}
