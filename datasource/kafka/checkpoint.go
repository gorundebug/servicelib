package kafka

import (
	"errors"
	"fmt"
	"sort"

	"github.com/IBM/sarama"
	"github.com/gorundebug/servicelib/runtime/config"
)

// ErrCheckpointRestore means no input may be admitted from an unconfirmed
// restoration position. A fresh service lifecycle must restore its state before
// retrying; ordinary transient broker failures retain the broker retry policy.
var ErrCheckpointRestore = errors.New("Kafka checkpoint restoration failed")

func validateCheckpoint(offsets map[int32]int64) error {
	for partition, next := range offsets {
		if partition < 0 || next < 0 {
			return fmt.Errorf("%w: invalid partition %d or next offset %d", ErrCheckpointRestore, partition, next)
		}
	}
	return nil
}

func (ep *saramaKafkaEndpoint) restoreAssignment(session sarama.ConsumerGroupSession) error {
	ctx := session.Context()
	offsets, err := ep.consumer.RestoreCheckpoint(ctx)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrCheckpointRestore, err)
	}
	if (offsets != nil) != ep.restoring {
		return fmt.Errorf("%w: recovery mode changed during the service lifecycle", ErrCheckpointRestore)
	}
	if !ep.restoring {
		return ctx.Err()
	}
	if err := validateCheckpoint(offsets); err != nil {
		return err
	}
	if ep.client == nil {
		return fmt.Errorf("%w: missing consumer client", ErrCheckpointRestore)
	}
	topic := ep.GetConfig().(*config.KafkaEndpointConfig).Topic
	selected := make(map[int32]int64)
	for assignedTopic, partitions := range session.Claims() {
		if assignedTopic != topic {
			return fmt.Errorf("%w: unexpected assigned topic", ErrCheckpointRestore)
		}
		for _, partition := range partitions {
			next, ok := offsets[partition]
			if !ok {
				return fmt.Errorf("%w: no restored state for partition %d", ErrCheckpointRestore, partition)
			}
			selected[partition] = next
		}
	}
	// Validate the whole assignment before changing any offset manager. No
	// partial restore may be mistaken for a successful application checkpoint.
	partitions := make([]int32, 0, len(selected))
	for partition := range selected {
		partitions = append(partitions, partition)
	}
	sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
	for _, partition := range partitions {
		if err := ctx.Err(); err != nil {
			return err
		}
		low, err := ep.client.GetOffset(topic, partition, sarama.OffsetOldest)
		if err != nil {
			return fmt.Errorf("%w: read retained boundary: %w", ErrCheckpointRestore, err)
		}
		high, err := ep.client.GetOffset(topic, partition, sarama.OffsetNewest)
		if err != nil {
			return fmt.Errorf("%w: read log end: %w", ErrCheckpointRestore, err)
		}
		next := selected[partition]
		if low < 0 || high < low || next < low || next > high {
			return fmt.Errorf("%w: partition %d checkpoint %d outside retained range [%d,%d]", ErrCheckpointRestore, partition, next, low, high)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, partition := range partitions {
		next := selected[partition]
		// Sarama exposes downward ResetOffset and upward MarkOffset. Together
		// they set the exact durable prefix, including an uninitialized group.
		// This deliberately permits committing that already restored prefix;
		// it is NOT a read-only seek and never acknowledges later records.
		session.ResetOffset(topic, partition, next, "")
		session.MarkOffset(topic, partition, next, "")
	}
	ep.restored = selected
	return nil
}
