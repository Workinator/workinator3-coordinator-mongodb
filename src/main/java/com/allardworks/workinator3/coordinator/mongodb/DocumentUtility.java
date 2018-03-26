package com.allardworks.workinator3.coordinator.mongodb;

import lombok.val;
import org.bson.Document;

public class DocumentUtility {
    private DocumentUtility() {
    }

    /**
     * This isn't pretty, but should be convenient.
     * Even elements are names (string), odds are values (whatever).
     * @param fieldsAndValues
     * @return
     */
    public static Document doc(Object... fieldsAndValues) {
        val doc = new Document();
        for (int i = 0; i < fieldsAndValues.length; i += 2) {
            doc.append((String)fieldsAndValues[i], fieldsAndValues[i+1]);
        }
        return doc;
    }
}