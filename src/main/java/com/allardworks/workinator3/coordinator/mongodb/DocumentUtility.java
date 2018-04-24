package com.allardworks.workinator3.coordinator.mongodb;

import lombok.val;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.addAll;

public class DocumentUtility {
    private DocumentUtility() {
    }

    /**
     * This isn't pretty, but should be convenient.
     * Even elements are names (string), odds are values (whatever).
     *
     * @param fieldsAndValues
     * @return
     */
    public static Document doc(Object... fieldsAndValues) {
        val doc = new Document();
        for (int i = 0; i < fieldsAndValues.length; i += 2) {
            doc.append((String) fieldsAndValues[i], fieldsAndValues[i + 1]);
        }
        return doc;
    }

    public static List<Document> list(Document... values) {
        val list = new ArrayList<Document>();
        addAll(list, values);
        return list;
    }
}