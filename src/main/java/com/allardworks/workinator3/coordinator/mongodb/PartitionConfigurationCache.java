package com.allardworks.workinator3.coordinator.mongodb;

import com.allardworks.workinator3.core.PartitionConfiguration;
import com.allardworks.workinator3.core.StupidCache;
import org.springframework.stereotype.Component;

/**
 * Created by jaya on 2/25/18.
 * Retrieves partition configuration objects from the database,
 * and stores them for up to 5 minutes.
 * Objects are lazy loaded one at a time as needed.
 */
@Component
public class PartitionConfigurationCache {
    private final StupidCache<String, PartitionConfiguration> partitionConfigurationStupidCache;

    public PartitionConfigurationCache(final MongoDal dal) {
        partitionConfigurationStupidCache = new StupidCache<>(dal::getPartitionConfiguration);
    }

    /**
     * Caches the partition configuration objects for 5 minutes each.
     */
    public PartitionConfiguration getConfiguration(final String partitionKey) {
        return partitionConfigurationStupidCache.getItem(partitionKey);
    }
}