/*
 * Copyright 2015 Realm Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.realm;

import android.os.Handler;
import android.os.HandlerThread;
import android.test.AndroidTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import io.realm.entities.Dog;

public class FineGrainedNotificationsTest extends AndroidTestCase {
    private HandlerThread handlerThread;
    private Handler handler;
    private CountDownLatch signalTestFinished;
    private AtomicInteger globalCommitInvocations;
    private AtomicInteger fineGrainedCommitInvocations;
    private RealmConfiguration configuration;
    private Realm realm;

    @Override
    protected void setUp() throws Exception {
        handlerThread = new HandlerThread("LooperThread");
        handlerThread.start();
        handler = new Handler(handlerThread.getLooper());
        signalTestFinished = new CountDownLatch(1);
        globalCommitInvocations = new AtomicInteger(0);
        fineGrainedCommitInvocations= new AtomicInteger(0);
        configuration = new RealmConfiguration.Builder(getContext()).build();
        Realm.deleteRealm(configuration);
    }

    @Override
    protected void tearDown() throws Exception {
        final CountDownLatch cleanup = new CountDownLatch(1);
        if (realm != null && handler.getLooper().getThread().isAlive()) {
            handler.post(new Runnable() {
                @Override
                public void run() {
                    if (!realm.isClosed()) {
                        realm.close();
                    }
                    Realm.deleteRealm(configuration);
                    handlerThread.quit();
                    realm = null;
                    cleanup.countDown();
                }
            });
            TestHelper.awaitOrFail(cleanup);
        }
    }

    // ********************************************************************************* //
    // UC 1.
    // Callback should be invoked after a relevant commit (one that should impact the
    // query from which we obtained our RealmObject or RealmResults)
    // ********************************************************************************* //

    // UC 1 for Sync RealmObject
    public void test_callback_with_relevant_commit_realmobject_sync() {
        handler.post(new Runnable() {
            @Override
            public void run() {
                realm = Realm.getInstance(configuration);
                realm.addChangeListener(new RealmChangeListener() {
                    @Override
                    public void onChange() {
                        if (globalCommitInvocations.incrementAndGet() == 3)  {
                            signalTestFinished.countDown();
                        }
                    }
                });

                realm.beginTransaction();
                Dog akamaru = realm.createObject(Dog.class);
                akamaru.setName("Akamaru");
                realm.commitTransaction();

                final Dog dog = realm.where(Dog.class).findFirst();
                dog.addChangeListener(new RealmChangeListener() {
                    @Override
                    public void onChange() {
                        assertEquals("Akamaru", dog.getName());
                        fineGrainedCommitInvocations.incrementAndGet();
                    }
                });

                // this commit should not trigger the fine-grained callback
                // it will re-run the query in the background though
                realm.beginTransaction();
                realm.commitTransaction();

                realm.beginTransaction();
                akamaru.setAge(17);
                realm.commitTransaction();
            }
        });
        TestHelper.awaitOrFail(signalTestFinished);
        assertEquals(1, fineGrainedCommitInvocations.get());
    }
    // UC 1 Async RealmObject
    public void test_callback_with_relevant_commit_realmobject_async() {
        handler.post(new Runnable() {
            @Override
            public void run() {
                realm = Realm.getInstance(configuration);
                realm.addChangeListener(new RealmChangeListener() {
                    @Override
                    public void onChange() {
                        if (globalCommitInvocations.incrementAndGet() == 3) {
                            signalTestFinished.countDown();
                        }
                    }
                });

                final Dog dog = realm.where(Dog.class).findFirstAsync();
                assertTrue(dog.load());
                dog.addChangeListener(new RealmChangeListener() {
                    @Override
                    public void onChange() {
                        assertEquals("Akamaru", dog.getName());
                        fineGrainedCommitInvocations.incrementAndGet();
                    }
                });

                realm.beginTransaction();
                realm.commitTransaction();

                realm.beginTransaction();
                Dog akamaru = realm.createObject(Dog.class);
                akamaru.setName("Akamaru");
                realm.commitTransaction();

                realm.beginTransaction();
                akamaru.setAge(17);
                realm.commitTransaction();

            }
        });
        TestHelper.awaitOrFail(signalTestFinished);
        assertEquals(2, fineGrainedCommitInvocations.get());
    }
    // UC 1 Sync RealmResults
    public void test_callback_with_relevant_commit_realmresults_sync() {
        handler.post(new Runnable() {
            @Override
            public void run() {
                realm = Realm.getInstance(configuration);
                realm.addChangeListener(new RealmChangeListener() {
                    @Override
                    public void onChange() {
                        if (globalCommitInvocations.incrementAndGet() == 3)  {
                            signalTestFinished.countDown();
                        }
                    }
                });

                realm.beginTransaction();
                Dog akamaru = realm.createObject(Dog.class);
                akamaru.setName("Akamaru");
                realm.commitTransaction();

                final RealmResults<Dog> dogs = realm.where(Dog.class).findAll();
                dogs.addChangeListener(new RealmChangeListener() {
                    @Override
                    public void onChange() {
                        assertEquals(1, dogs.size());
                        assertEquals("Akamaru", dogs.get(0).getName());
                        fineGrainedCommitInvocations.incrementAndGet();
                    }
                });

                realm.beginTransaction();
                realm.commitTransaction();

                realm.beginTransaction();
                akamaru.setAge(17);
                realm.commitTransaction();
            }
        });
        TestHelper.awaitOrFail(signalTestFinished);
        assertEquals(1, fineGrainedCommitInvocations.get());
    }
    // UC 1 Async RealmResults
    public void test_callback_with_relevant_commit_realmresults_async() {
        handler.post(new Runnable() {
            @Override
            public void run() {
                realm = Realm.getInstance(configuration);
                realm.addChangeListener(new RealmChangeListener() {
                    @Override
                    public void onChange() {
                        if (globalCommitInvocations.incrementAndGet() == 3)  {
                            signalTestFinished.countDown();
                        }
                    }
                });

                final RealmResults<Dog> dogs = realm.where(Dog.class).findAllAsync();
                assertTrue(dogs.load());
                dogs.addChangeListener(new RealmChangeListener() {
                    @Override
                    public void onChange() {
                        assertEquals(1, dogs.size());
                        assertEquals("Akamaru", dogs.get(0).getName());
                        fineGrainedCommitInvocations.incrementAndGet();
                    }
                });

                realm.beginTransaction();
                realm.commitTransaction();

                realm.beginTransaction();
                Dog akamaru = realm.createObject(Dog.class);
                akamaru.setName("Akamaru");
                realm.commitTransaction();

                realm.beginTransaction();
                akamaru.setAge(17);
                realm.commitTransaction();

            }
        });
        TestHelper.awaitOrFail(signalTestFinished);
        assertEquals(2, fineGrainedCommitInvocations.get());
    }

    // ********************************************************************************* //
    // UC 2.
    // Multiple callbacks should be invoked after a relevant commit
    // ********************************************************************************* //

    // UC 2 for Sync RealmObject
    public void test_multiple_callbacks_should_be_invoked_realmobject_sync() {
        final int NUMBER_OF_LISTENERS = 7;
        handler.post(new Runnable() {
            @Override
            public void run() {
                realm = Realm.getInstance(configuration);
                RealmChangeListener listener = new RealmChangeListener() {
                    @Override
                    public void onChange() {
                        if (globalCommitInvocations.incrementAndGet() == 3)  {
                            signalTestFinished.countDown();
                        }
                    }
                };

                realm.addChangeListener(listener);

                realm.beginTransaction();
                Dog akamaru = realm.createObject(Dog.class);
                realm.commitTransaction();

                Dog dog = realm.where(Dog.class).findFirst();
                for (int i = 0; i < NUMBER_OF_LISTENERS; i++) {
                    dog.addChangeListener(new RealmChangeListener() {
                        @Override
                        public void onChange() {
                            fineGrainedCommitInvocations.incrementAndGet();
                        }
                    });
                }

                realm.beginTransaction();
                realm.commitTransaction();

                realm.beginTransaction();
                akamaru.setAge(17);
                realm.commitTransaction();
            }
        });
        TestHelper.awaitOrFail(signalTestFinished);
        assertEquals(NUMBER_OF_LISTENERS, fineGrainedCommitInvocations.get());
    }

    // UC 2 Async RealmObject
    public void test_multiple_callbacks_should_be_invoked_realmobject_async() {
        final int NUMBER_OF_LISTENERS = 7;
        handler.post(new Runnable() {
            @Override
            public void run() {
                realm = Realm.getInstance(configuration);
                RealmChangeListener listener = new RealmChangeListener() {
                    @Override
                    public void onChange() {
                        if (globalCommitInvocations.incrementAndGet() == 3)  {
                            signalTestFinished.countDown();
                        }
                    }
                };

                realm.addChangeListener(listener);

                realm.beginTransaction();
                Dog akamaru = realm.createObject(Dog.class);
                realm.commitTransaction();

                Dog dog = realm.where(Dog.class).findFirstAsync();
                assertTrue(dog.load());
                for (int i = 0; i < NUMBER_OF_LISTENERS; i++) {
                    dog.addChangeListener(new RealmChangeListener() {
                        @Override
                        public void onChange() {
                            fineGrainedCommitInvocations.incrementAndGet();
                        }
                    });
                }

                realm.beginTransaction();
                realm.commitTransaction();

                realm.beginTransaction();
                akamaru.setAge(17);
                realm.commitTransaction();
            }
        });
        TestHelper.awaitOrFail(signalTestFinished);
        assertEquals(NUMBER_OF_LISTENERS, fineGrainedCommitInvocations.get());
    }

    // UC 2 Sync RealmResults
    public void test_multiple_callbacks_should_be_invoked_realmresults_sync() {
        final int NUMBER_OF_LISTENERS = 7;
        handler.post(new Runnable() {
            @Override
            public void run() {
                realm = Realm.getInstance(configuration);
                RealmChangeListener listener = new RealmChangeListener() {
                    @Override
                    public void onChange() {
                        if (globalCommitInvocations.incrementAndGet() == 3)  {
                            signalTestFinished.countDown();
                        }
                    }
                };

                realm.addChangeListener(listener);

                realm.beginTransaction();
                Dog akamaru = realm.createObject(Dog.class);
                realm.commitTransaction();

                RealmResults<Dog> dogs = realm.where(Dog.class).findAll();
                for (int i = 0; i < NUMBER_OF_LISTENERS; i++) {
                    dogs.addChangeListener(new RealmChangeListener() {
                        @Override
                        public void onChange() {
                            fineGrainedCommitInvocations.incrementAndGet();
                        }
                    });
                }

                realm.beginTransaction();
                realm.commitTransaction();

                realm.beginTransaction();
                akamaru.setAge(17);
                realm.commitTransaction();
            }
        });
        TestHelper.awaitOrFail(signalTestFinished);
        assertEquals(NUMBER_OF_LISTENERS, fineGrainedCommitInvocations.get());
    }

    // UC 2 Async RealmResults
    public void test_multiple_callbacks_should_be_invoked_realmresults_async() {
        final int NUMBER_OF_LISTENERS = 7;
        handler.post(new Runnable() {
            @Override
            public void run() {
                realm = Realm.getInstance(configuration);
                RealmChangeListener listener = new RealmChangeListener() {
                    @Override
                    public void onChange() {
                        if (globalCommitInvocations.incrementAndGet() == 3)  {
                            signalTestFinished.countDown();
                        }
                    }
                };

                realm.addChangeListener(listener);

                realm.beginTransaction();
                Dog akamaru = realm.createObject(Dog.class);
                realm.commitTransaction();

                RealmResults<Dog> dogs = realm.where(Dog.class).findAllAsync();
                assertTrue(dogs.load());
                for (int i = 0; i < NUMBER_OF_LISTENERS; i++) {
                    dogs.addChangeListener(new RealmChangeListener() {
                        @Override
                        public void onChange() {
                            fineGrainedCommitInvocations.incrementAndGet();
                        }
                    });
                }

                realm.beginTransaction();
                realm.commitTransaction();

                realm.beginTransaction();
                akamaru.setAge(17);
                realm.commitTransaction();
            }
        });
        TestHelper.awaitOrFail(signalTestFinished);
        assertEquals(NUMBER_OF_LISTENERS, fineGrainedCommitInvocations.get());
    }

    // ********************************************************************************* //
    // UC 3.
    // Callback should be invoked when a non Looper thread commits
    // ********************************************************************************* //

    // UC 3 for Sync RealmObject
    public void test_non_looper_thread_commit_realmobject_sync() {
        handler.post(new Runnable() {
            @Override
            public void run() {
                realm = Realm.getInstance(configuration);
                realm.addChangeListener(new RealmChangeListener() {
                    @Override
                    public void onChange() {
                        if (globalCommitInvocations.incrementAndGet() == 2) {
                            signalTestFinished.countDown();
                        }
                    }
                });

                realm.beginTransaction();
                realm.createObject(Dog.class);
                realm.commitTransaction();

                Dog dog = realm.where(Dog.class).findFirst();
                dog.addChangeListener(new RealmChangeListener() {
                    @Override
                    public void onChange() {
                        fineGrainedCommitInvocations.incrementAndGet();
                    }
                });

                Thread thread = new Thread() {
                    @Override
                    public void run() {
                        Realm bgRealm = Realm.getInstance(realm.getConfiguration());
                        bgRealm.beginTransaction();
                        bgRealm.createObject(Dog.class);
                        bgRealm.commitTransaction();
                        bgRealm.close();
                    }
                };
                thread.start();
                try {
                    thread.join();
                    // this will give the posted notification a chance to execute
                    // keep this Runnable alive (waiting for the commit to arrive)
                    final int MAX_RETRIES = 60;
                    int numberOfSleep = 0;
                    while (numberOfSleep++ < MAX_RETRIES
                            && fineGrainedCommitInvocations.incrementAndGet() != 1) {
                        Thread.sleep(16);
                    }
                    assertEquals(1, fineGrainedCommitInvocations.get());
                } catch (InterruptedException e) {
                    fail(e.getMessage());
                }
            }
        });
        TestHelper.awaitOrFail(signalTestFinished);
        assertEquals(1, fineGrainedCommitInvocations.get());
    }
    // UC 3 Async RealmObject
    public void test_non_looper_thread_commit_realmobject_async() {
        handler.post(new Runnable() {
            @Override
            public void run() {
                realm = Realm.getInstance(configuration);
                realm.addChangeListener(new RealmChangeListener() {
                    @Override
                    public void onChange() {
                        if (globalCommitInvocations.incrementAndGet() == 2) {
                            signalTestFinished.countDown();
                        }
                    }
                });

                realm.beginTransaction();
                realm.createObject(Dog.class);
                realm.commitTransaction();

                Dog dog = realm.where(Dog.class).findFirstAsync();
                assertTrue(dog.load());
                dog.addChangeListener(new RealmChangeListener() {
                    @Override
                    public void onChange() {
                        fineGrainedCommitInvocations.incrementAndGet();
                    }
                });

                Thread thread = new Thread() {
                    @Override
                    public void run() {
                        Realm bgRealm = Realm.getInstance(realm.getConfiguration());
                        bgRealm.beginTransaction();
                        bgRealm.createObject(Dog.class);
                        bgRealm.commitTransaction();
                        bgRealm.close();
                    }
                };
                thread.start();
                try {
                    thread.join();

                    final int MAX_RETRIES = 60;
                    int numberOfSleep = 0;
                    while (numberOfSleep++ < MAX_RETRIES
                            && fineGrainedCommitInvocations.incrementAndGet() != 1) {
                        Thread.sleep(16);
                    }
                    assertEquals(1, fineGrainedCommitInvocations.get());
                } catch (InterruptedException e) {
                    fail(e.getMessage());
                }
            }
        });
        TestHelper.awaitOrFail(signalTestFinished);
        assertEquals(1, fineGrainedCommitInvocations.get());
    }
    // UC 3 Sync RealmResults
    public void test_non_looper_thread_commit_realmresults_sync() {
        handler.post(new Runnable() {
            @Override
            public void run() {
                realm = Realm.getInstance(configuration);
                realm.addChangeListener(new RealmChangeListener() {
                    @Override
                    public void onChange() {
                        if (globalCommitInvocations.incrementAndGet() == 2) {
                            signalTestFinished.countDown();
                        }
                    }
                });

                realm.beginTransaction();
                realm.createObject(Dog.class);
                realm.commitTransaction();

                final RealmResults<Dog> dogs = realm.where(Dog.class).findAll();
                dogs.addChangeListener(new RealmChangeListener() {
                    @Override
                    public void onChange() {
                        fineGrainedCommitInvocations.incrementAndGet();
                        assertEquals(2, dogs.size());
                    }
                });

                Thread thread = new Thread() {
                    @Override
                    public void run() {
                        Realm bgRealm = Realm.getInstance(realm.getConfiguration());
                        bgRealm.beginTransaction();
                        bgRealm.createObject(Dog.class);
                        bgRealm.commitTransaction();
                        bgRealm.close();
                    }
                };
                thread.start();
                try {
                    thread.join();

                    final int MAX_RETRIES = 60;
                    int numberOfSleep = 0;
                    while (numberOfSleep++ < MAX_RETRIES
                            && fineGrainedCommitInvocations.incrementAndGet() != 1) {
                        Thread.sleep(16);
                    }
                    assertEquals(1, fineGrainedCommitInvocations.get());
                } catch (InterruptedException e) {
                    fail(e.getMessage());
                }
            }
        });
        TestHelper.awaitOrFail(signalTestFinished);
    }
    // UC 3 Async RealmResults
    public void test_non_looper_thread_commit_realmresults_async() {
        handler.post(new Runnable() {
            @Override
            public void run() {
                realm = Realm.getInstance(configuration);
                realm.addChangeListener(new RealmChangeListener() {
                    @Override
                    public void onChange() {
                        if (globalCommitInvocations.incrementAndGet() == 2) {
                            signalTestFinished.countDown();
                        }
                    }
                });

                realm.beginTransaction();
                realm.createObject(Dog.class);
                realm.commitTransaction();

                final RealmResults<Dog> dogs = realm.where(Dog.class).findAllAsync();
                assertTrue(dogs.load());
                dogs.addChangeListener(new RealmChangeListener() {
                    @Override
                    public void onChange() {
                        fineGrainedCommitInvocations.incrementAndGet();
                        assertEquals(2, dogs.size());
                    }
                });

                Thread thread = new Thread() {
                    @Override
                    public void run() {
                        Realm bgRealm = Realm.getInstance(realm.getConfiguration());
                        bgRealm.beginTransaction();
                        bgRealm.createObject(Dog.class);
                        bgRealm.commitTransaction();
                        bgRealm.close();
                    }
                };
                thread.start();
                try {
                    thread.join();

                    final int MAX_RETRIES = 60;
                    int numberOfSleep = 0;
                    while (numberOfSleep++ < MAX_RETRIES
                            && fineGrainedCommitInvocations.incrementAndGet() != 1) {
                        Thread.sleep(16);
                    }
                    assertEquals(1, fineGrainedCommitInvocations.get());
                } catch (InterruptedException e) {
                    fail(e.getMessage());
                }

            }
        });
        TestHelper.awaitOrFail(signalTestFinished);
        assertEquals(1, fineGrainedCommitInvocations.get());
    }

    // ****************************************************************************************** //
    // UC 4.
    // Callback should throw if registered on a non Looper thread.
    // no tests for async RealmObject & RealmResults, since those already require a Looper thread
    // ***************************************************************************************** //

    // UC 4 for Realm
    public void test_should_throw_on_non_looper_thread_realm() {
        new Thread() {
            @Override
            public void run() {
                Realm bgRealm = Realm.getInstance(configuration);
                try {
                    bgRealm.addChangeListener(new RealmChangeListener() {
                        @Override
                        public void onChange() {
                            fail("Callback should not be registered and invoked on a non-Looper thread");
                        }
                    });
                    fail("Callback should not be registered and invoked on a non-Looper thread");
                } catch (IllegalStateException ignored) {

                } finally {
                    bgRealm.close();
                    signalTestFinished.countDown();
                }
            }
        }.start();
        TestHelper.awaitOrFail(signalTestFinished);
    }

    // UC 4 for RealmObject
    public void test_should_throw_on_non_looper_thread_realmobject() {
        new Thread() {
            @Override
            public void run() {
                Realm bgRealm = Realm.getInstance(configuration);
                try {
                    Dog dog = bgRealm.where(Dog.class).findFirst();
                    dog.addChangeListener(new RealmChangeListener() {
                        @Override
                        public void onChange() {
                            fail("Callback should not be registered and invoked on a non-Looper thread");
                        }
                    });
                    fail("Callback should not be registered and invoked on a non-Looper thread");
                } catch (IllegalStateException ignored) {

                } finally {
                    bgRealm.close();
                    signalTestFinished.countDown();
                }
            }
        }.start();
        TestHelper.awaitOrFail(signalTestFinished);
    }
    // UC 4 RealmObject
    public void test_should_throw_on_non_looper_thread_realmoresults() {
        new Thread() {
            @Override
            public void run() {
                Realm bgRealm = Realm.getInstance(configuration);
                try {
                    RealmResults<Dog> dogs = bgRealm.where(Dog.class).findAll();
                    dogs.addChangeListener(new RealmChangeListener() {
                        @Override
                        public void onChange() {
                            fail("Callback should not be registered and invoked on a non-Looper thread");
                        }
                    });
                    fail("Callback should not be registered and invoked on a non-Looper thread");
                } catch (IllegalStateException ignored) {

                } finally {
                    bgRealm.close();
                    signalTestFinished.countDown();
                }
            }
        }.start();
        TestHelper.awaitOrFail(signalTestFinished);
    }
}
