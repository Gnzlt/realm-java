package io.realm.internal;

import java.util.IdentityHashMap;

/**
 * Created by Nabil on 05/11/15.
 */
public class CanonicalIdentityMap<K> extends IdentityHashMap<K,Integer> {
    private final static Integer PLACE_HOLDER = 0;

    public void put(K key)  {
        put(key, PLACE_HOLDER);
    }
}
