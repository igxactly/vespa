// Copyright 2016 Yahoo Inc. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.processing.request.test;

import com.yahoo.processing.request.properties.PropertyMap;
import com.yahoo.processing.request.properties.PublicCloneable;

import java.util.Collections;
import java.util.List;

/**
 * @author  bratseth
 */
public class PropertyMapTestCase extends junit.framework.TestCase {

    public void testCloning() {
        PropertyMap map=new PropertyMap();
        map.set("clonable",new ClonableObject());
        map.set("publicClonable",new PublicClonableObject());
        map.set("nonclonable",new NonClonableObject());
        map.set("clonableArray",new ClonableObject[] {new ClonableObject()});
        map.set("publicClonableArray",new ClonableObject[] {new ClonableObject()});
        map.set("nonclonableArray",new NonClonableObject[] {new NonClonableObject()});
        map.set("clonableList", Collections.singletonList(new ClonableObject()));
        map.set("nonclonableList", Collections.singletonList(new NonClonableObject()));
        assertNotNull(map.get("clonable"));
        assertNotNull(map.get("nonclonable"));

        PropertyMap mapClone=map.clone();
        assertTrue(map.get("clonable")      != mapClone.get("clonable"));
        assertTrue(map.get("publicClonable")!= mapClone.get("publicClonable"));
        assertTrue(map.get("nonclonable")   == mapClone.get("nonclonable"));

        assertTrue(map.get("clonableArray") != mapClone.get("clonableArray"));
        assertTrue(first(map.get("clonableArray")) != first(mapClone.get("clonableArray")));
        assertTrue(map.get("publicClonableArray") != mapClone.get("publicClonableArray"));
        assertTrue(first(map.get("publicClonableArray")) != first(mapClone.get("publicClonableArray")));
        assertTrue(first(map.get("nonclonableArray")) == first(mapClone.get("nonclonableArray")));
    }

    private Object first(Object object) {
        if (object instanceof Object[])
            return ((Object[])object)[0];
        if (object instanceof List)
            return ((List<?>)object).get(0);
        throw new IllegalArgumentException();
    }

    public static class ClonableObject implements Cloneable {

        @Override
        public ClonableObject clone() {
            try {
                return (ClonableObject)super.clone();
            }
            catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        }

    }

    public static class PublicClonableObject implements PublicCloneable<PublicClonableObject> {

        @Override
        public PublicClonableObject clone() {
            try {
                return (PublicClonableObject)super.clone();
            }
            catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        }

    }

    private static class NonClonableObject {

    }


}
