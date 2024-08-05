package structs

type Partition struct {
	Number int32
}

type Topic struct {
	Name            string
	NumOfPartitions int
	Partitions      []Partition
}

type Group struct {
	Name   string
	Topics []Topic
}

// OffsetTimestamp holds the offset and corresponding timestamp
type OffsetTimestamp struct {
	Offset    int64
	Timestamp int64
}
