// Copyright 2016 Yahoo Inc. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.search.query.profile;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.yahoo.search.query.profile.types.QueryProfileType;

import java.util.*;

/**
 * A variant of a query profile
 *
 * @author bratseth
*/
public class QueryProfileVariant implements Cloneable, Comparable<QueryProfileVariant> {

    private List<QueryProfile> inherited=null;

    private DimensionValues dimensionValues;

    private Map<String,Object> values;

    private boolean frozen=false;

    private QueryProfile owner;

    public QueryProfileVariant(DimensionValues dimensionValues, QueryProfile owner) {
        this.dimensionValues=dimensionValues;
        this.owner = owner;
    }

    public DimensionValues getDimensionValues() { return dimensionValues; }

    /**
     * Returns the live reference to the values of this. This may be modified
     * if this is not frozen.
     */
    public Map<String,Object> values() {
        if (values==null) {
            if (frozen)
                return Collections.emptyMap();
            else
                values=new HashMap<>();
        }
        return values;
    }

    /**
     * Returns the live reference to the inherited profiles of this. This may be modified
     * if this is not frozen.
     */
    public List<QueryProfile> inherited() {
        if (inherited==null) {
            if (frozen)
                return Collections.emptyList();
            else
                inherited=new ArrayList<>();
        }
        return inherited;
    }

    public void set(String key, Object newValue) {
        if (values==null)
            values=new HashMap<>();

        Object oldValue = values.get(key);

        if (oldValue == null) {
            values.put(key, newValue);
        } else {
            Object combinedOrNull = QueryProfile.combineValues(newValue, oldValue);
            if (combinedOrNull != null) {
                values.put(key, combinedOrNull);
            }
        }
    }

    public void inherit(QueryProfile profile) {
        if (inherited==null)
            inherited=new ArrayList<>(1);
        inherited.add(profile);
    }

    /**
     * Implements the sort order of this which is based on specificity
     * where dimensions to the left are more significant.
     * <p>
     * <b>Note:</b> This ordering is not consistent with equals - it returns 0 when the same dimensions
     * are <i>set</i>, regardless of what they are set <i>to</i>.
     */
    public @Override int compareTo(QueryProfileVariant other) {
        return this.dimensionValues.compareTo(other.dimensionValues);
    }

    public boolean matches(DimensionValues givenDimensionValues) {
        return this.dimensionValues.matches(givenDimensionValues);
    }

    /** Accepts a visitor to the values of this */
    public void accept(boolean allowContent,QueryProfileType type,QueryProfileVisitor visitor, DimensionBinding dimensionBinding) {
        // Visit this
        if (allowContent) {
            String key=visitor.getLocalKey();
            if (key!=null) {
                if (type!=null)
                    type.unalias(key);

                visitor.acceptValue(key, values().get(key), dimensionBinding, owner);
                if (visitor.isDone()) return;
            }
            else {
                for (Map.Entry<String,Object> entry : values().entrySet()) {
                    visitor.acceptValue(entry.getKey(), entry.getValue(), dimensionBinding, owner);
                    if (visitor.isDone()) return;
                }
            }
        }

        // Visit inherited
        for (QueryProfile profile : inherited()) {
            if (visitor.visitInherited()) {
                profile.accept(allowContent,visitor,dimensionBinding.createFor(profile.getDimensions()), owner);
            }
            if (visitor.isDone()) return;
        }
    }

    public void freeze() {
        if (frozen) return;
        if (inherited != null)
            inherited = ImmutableList.copyOf(inherited);
        if (values != null)
            values = ImmutableMap.copyOf(values);
        frozen=true;
    }

    public QueryProfileVariant clone() {
        if (frozen) return this;
       try {
           QueryProfileVariant clone=(QueryProfileVariant)super.clone();
           if (this.inherited!=null)
               clone.inherited=new ArrayList<>(this.inherited); // TODO: Deep clone is more correct, but probably does not matter in practice

           clone.values=CopyOnWriteContent.deepClone(this.values);

           return clone;
       }
       catch (CloneNotSupportedException e) {
           throw new RuntimeException(e);
       }
    }

    public @Override String toString() {
        return "query profile variant for " + dimensionValues;
    }

}
