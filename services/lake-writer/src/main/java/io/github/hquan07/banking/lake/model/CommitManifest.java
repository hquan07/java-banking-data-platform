package io.github.hquan07.banking.lake.model;

import java.util.List;

public record CommitManifest(
        String batchId,
        String layer,
        String status,
        String committedAt,
        List<OffsetRange> sourceOffsets,
        List<LakeObjectDescriptor> objects
) {
}
