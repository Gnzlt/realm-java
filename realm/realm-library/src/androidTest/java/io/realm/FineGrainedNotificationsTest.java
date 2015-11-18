package io.realm;

import android.os.Handler;
import android.os.HandlerThread;
import android.os.Looper;
import android.os.MessageQueue;
import android.os.SystemClock;
import android.test.AndroidTestCase;
import android.test.InstrumentationTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import io.realm.entities.Dog;

/**
 * Created by Nabil on 12/11/15.
 */
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
        if (realm != null) {
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
        }
        TestHelper.awaitOrFail(cleanup);
    }

    // ********************************************************************************* //
    // UC 1.
    // Callback should be invoked after a relevant commit (one that should impact the
    // query from which we obtained our RealmObject or RealmResults)
    // ********************************************************************************* //

    // UC 1 for Sync RealmObject
    public void test_callback_with_relevant_commit_realmobject_sync () {
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

                RealmChangeListener listenerFineGrained = new RealmChangeListener() {
                    @Override
                    public void onChange() {
                        fineGrainedCommitInvocations.incrementAndGet();
                    }
                };
                realm.addChangeListener(listener);

                realm.beginTransaction();
                Dog akamaru = realm.createObject(Dog.class);
                akamaru.setName("Akamaru");
                realm.commitTransaction();

                Dog dog = realm.where(Dog.class).findFirst();
                dog.addChangeListener(listenerFineGrained);

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
    public void test_callback_with_relevant_commit_realmobject_async () {
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

                RealmChangeListener listenerFineGrained = new RealmChangeListener() {
                    @Override
                    public void onChange() {
                        fineGrainedCommitInvocations.incrementAndGet();
                    }
                };
                realm.addChangeListener(listener);


                Dog dog = realm.where(Dog.class).findFirstAsync();
                assertTrue(dog.load());
                dog.addChangeListener(listenerFineGrained);

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
    public void test_callback_with_relevant_commit_realmresults_sync () {
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

                RealmChangeListener listenerFineGrained = new RealmChangeListener() {
                    @Override
                    public void onChange() {
                        fineGrainedCommitInvocations.incrementAndGet();
                    }
                };
                realm.addChangeListener(listener);

                realm.beginTransaction();
                Dog akamaru = realm.createObject(Dog.class);
                akamaru.setName("Akamaru");
                realm.commitTransaction();

                RealmResults<Dog> dogs = realm.where(Dog.class).findAll();
                dogs.addChangeListener(listenerFineGrained);

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
    public void test_callback_with_relevant_commit_realmresults_async () {
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

                RealmChangeListener listenerFineGrained = new RealmChangeListener() {
                    @Override
                    public void onChange() {
                        fineGrainedCommitInvocations.incrementAndGet();
                    }
                };
                realm.addChangeListener(listener);


                RealmResults<Dog> dogs = realm.where(Dog.class).findAllAsync();
                assertTrue(dogs.load());
                dogs.addChangeListener(listenerFineGrained);

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
    // Callback should not be invoked after a commit (one that should not impact the
    // query from which we obtained our RealmObject or RealmResults)
    // ********************************************************************************* //

    // UC 2 for Sync RealmObject
    // UC 2 Async RealmObject
    // UC 2 Sync RealmResults
    // UC 2 Async RealmResults


    // ********************************************************************************* //
    // UC 3.
    // Multiple callbacks should be invoked after a relevant commit
    // ********************************************************************************* //

    // UC 3 for Sync RealmObject
    public void test_multiple_callbacks_should_be_invoked_realmobject_sync () {
        final RealmChangeListener[] fineGrainedCommitInvocationsCallbacks = new RealmChangeListener[7];
        for (int i = 0; i < fineGrainedCommitInvocationsCallbacks.length; i++) {
            fineGrainedCommitInvocationsCallbacks[i] = new RealmChangeListener() {
                @Override
                public void onChange() {
                    fineGrainedCommitInvocations.incrementAndGet();
                }
            };
        }

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
                akamaru.setName("Akamaru");
                realm.commitTransaction();

                Dog dog = realm.where(Dog.class).findFirst();
                for (int i = 0; i < fineGrainedCommitInvocationsCallbacks.length; i++) {
                    dog.addChangeListener(fineGrainedCommitInvocationsCallbacks[i]);
                }

                realm.beginTransaction();
                realm.commitTransaction();

                realm.beginTransaction();
                akamaru.setAge(17);
                realm.commitTransaction();
            }
        });
        TestHelper.awaitOrFail(signalTestFinished);
        assertEquals(7, fineGrainedCommitInvocations.get());
    }

    // UC 3 Async RealmObject
    public void test_multiple_callbacks_should_be_invoked_realmobject_async () {
        final RealmChangeListener[] fineGrainedCommitInvocationsCallbacks = new RealmChangeListener[7];
        for (int i = 0; i < fineGrainedCommitInvocationsCallbacks.length; i++) {
            fineGrainedCommitInvocationsCallbacks[i] = new RealmChangeListener() {
                @Override
                public void onChange() {
                    fineGrainedCommitInvocations.incrementAndGet();
                }
            };
        }

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
                akamaru.setName("Akamaru");
                realm.commitTransaction();

                Dog dog = realm.where(Dog.class).findFirstAsync();
                assertTrue(dog.load());
                for (int i = 0; i < fineGrainedCommitInvocationsCallbacks.length; i++) {
                    dog.addChangeListener(fineGrainedCommitInvocationsCallbacks[i]);
                }

                realm.beginTransaction();
                realm.commitTransaction();

                realm.beginTransaction();
                akamaru.setAge(17);
                realm.commitTransaction();
            }
        });
        TestHelper.awaitOrFail(signalTestFinished);
        assertEquals(7, fineGrainedCommitInvocations.get());
    }

    // UC 3 Sync RealmResults
    public void test_multiple_callbacks_should_be_invoked_realmresults_sync () {
        final RealmChangeListener[] fineGrainedCommitInvocationsCallbacks = new RealmChangeListener[7];
        for (int i = 0; i < fineGrainedCommitInvocationsCallbacks.length; i++) {
            fineGrainedCommitInvocationsCallbacks[i] = new RealmChangeListener() {
                @Override
                public void onChange() {
                    fineGrainedCommitInvocations.incrementAndGet();
                }
            };
        }

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
                akamaru.setName("Akamaru");
                realm.commitTransaction();

                RealmResults<Dog> dogs = realm.where(Dog.class).findAll();
                for (int i = 0; i < fineGrainedCommitInvocationsCallbacks.length; i++) {
                    dogs.addChangeListener(fineGrainedCommitInvocationsCallbacks[i]);
                }

                realm.beginTransaction();
                realm.commitTransaction();

                realm.beginTransaction();
                akamaru.setAge(17);
                realm.commitTransaction();
            }
        });
        TestHelper.awaitOrFail(signalTestFinished);
        assertEquals(7, fineGrainedCommitInvocations.get());
    }

    // UC 3 Async RealmResults
    public void test_multiple_callbacks_should_be_invoked_realmresults_async () {
        final RealmChangeListener[] fineGrainedCommitInvocationsCallbacks = new RealmChangeListener[7];
        for (int i = 0; i < fineGrainedCommitInvocationsCallbacks.length; i++) {
            fineGrainedCommitInvocationsCallbacks[i] = new RealmChangeListener() {
                @Override
                public void onChange() {
                    fineGrainedCommitInvocations.incrementAndGet();
                }
            };
        }

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
                akamaru.setName("Akamaru");
                realm.commitTransaction();

                RealmResults<Dog> dogs = realm.where(Dog.class).findAllAsync();
                assertTrue(dogs.load());
                for (int i = 0; i < fineGrainedCommitInvocationsCallbacks.length; i++) {
                    dogs.addChangeListener(fineGrainedCommitInvocationsCallbacks[i]);
                }

                realm.beginTransaction();
                realm.commitTransaction();

                realm.beginTransaction();
                akamaru.setAge(17);
                realm.commitTransaction();
            }
        });
        TestHelper.awaitOrFail(signalTestFinished);
        assertEquals(7, fineGrainedCommitInvocations.get());
    }

    // ********************************************************************************* //
    // UC 4.
    // Callbacks should not be invoked while the RealmObject/RealmResults is sill empty.
    // updating the Realm with irrelevant commit should still not trigger the callback
    // adding a relevant commit will trigger callback
    // ********************************************************************************* //

    // UC 4 for Sync RealmObject
    // UC 4 Async RealmObject
    // UC 4 Sync RealmResults
    // UC 4 Async RealmResults

    // ********************************************************************************* //
    // UC 5.
    // Callback should be invoked when a non Looper thread commit
    // ********************************************************************************* //

    // UC 5 for Sync RealmObject
    public void test_non_looper_thread_commit_realmobject_sync () {
        final CountDownLatch bgNonLooperThread = new CountDownLatch(1);
        handler.post(new Runnable() {
            @Override
            public void run() {
                realm = Realm.getInstance(configuration);
                RealmChangeListener listener = new RealmChangeListener() {
                    @Override
                    public void onChange() {
                        if (globalCommitInvocations.incrementAndGet() == 2)  {
                            signalTestFinished.countDown();
                        }
                    }
                };

                RealmChangeListener listenerFineGrained = new RealmChangeListener() {
                    @Override
                    public void onChange() {
                        fineGrainedCommitInvocations.incrementAndGet();
                    }
                };
                realm.addChangeListener(listener);

                realm.beginTransaction();
                Dog akamaru = realm.createObject(Dog.class);
                akamaru.setName("Akamaru");
                realm.commitTransaction();

                Dog dog = realm.where(Dog.class).findFirst();
                dog.addChangeListener(listenerFineGrained);

                new Thread () {
                    @Override
                    public void run() {
                        Realm bgRealm = Realm.getInstance(realm.getConfiguration());
                        bgRealm.beginTransaction();
                        bgRealm.createObject(Dog.class);
                        bgRealm.commitTransaction();
                        bgRealm.close();
                        bgNonLooperThread.countDown();
                    }
                }.start();
            }
        });
        TestHelper.awaitOrFail(bgNonLooperThread);
        TestHelper.awaitOrFail(signalTestFinished);
        assertEquals(1, fineGrainedCommitInvocations.get());
    }
    // UC 5 Async RealmObject
    public void test_non_looper_thread_commit_realmobject_async () {
        final CountDownLatch bgNonLooperThread = new CountDownLatch(1);
        handler.post(new Runnable() {
            @Override
            public void run() {
                realm = Realm.getInstance(configuration);
                RealmChangeListener listener = new RealmChangeListener() {
                    @Override
                    public void onChange() {
                        if (globalCommitInvocations.incrementAndGet() == 2)  {
                            signalTestFinished.countDown();
                        }
                    }
                };

                RealmChangeListener listenerFineGrained = new RealmChangeListener() {
                    @Override
                    public void onChange() {
                        fineGrainedCommitInvocations.incrementAndGet();
                    }
                };
                realm.addChangeListener(listener);

                realm.beginTransaction();
                Dog akamaru = realm.createObject(Dog.class);
                akamaru.setName("Akamaru");
                realm.commitTransaction();

                Dog dog = realm.where(Dog.class).findFirstAsync();
                assertTrue(dog.load());
                dog.addChangeListener(listenerFineGrained);

                new Thread () {
                    @Override
                    public void run() {
                        Realm bgRealm = Realm.getInstance(realm.getConfiguration());
                        bgRealm.beginTransaction();
                        bgRealm.createObject(Dog.class);
                        bgRealm.commitTransaction();
                        bgRealm.close();
                        bgNonLooperThread.countDown();
                    }
                }.start();
            }
        });
        TestHelper.awaitOrFail(bgNonLooperThread);
        TestHelper.awaitOrFail(signalTestFinished);
        assertEquals(1, fineGrainedCommitInvocations.get());
    }
    // UC 5 Sync RealmResults
    public void test_non_looper_thread_commit_realmresults_sync () {
        final CountDownLatch bgNonLooperThread = new CountDownLatch(1);
        // Keep strong reference to the callback
        // TODO remove this once the PR about strong reference is merged
        final RealmChangeListener[] callbacks = new RealmChangeListener[2];
        handler.post(new Runnable() {
            @Override
            public void run() {
                realm = Realm.getInstance(configuration);

                RealmChangeListener listener = new RealmChangeListener() {
                    @Override
                    public void onChange() {
                        if (globalCommitInvocations.incrementAndGet() == 2) {
                            signalTestFinished.countDown();
                        }
                    }
                };
                realm.addChangeListener(listener);
                callbacks[0] = listener;

                realm.beginTransaction();
                Dog akamaru = realm.createObject(Dog.class);
                akamaru.setName("Akamaru");
                realm.commitTransaction();

                final RealmResults<Dog> dogs = realm.where(Dog.class).findAll();
                RealmChangeListener listenerFineGrained = new RealmChangeListener() {
                    @Override
                    public void onChange() {
                        fineGrainedCommitInvocations.incrementAndGet();
                        assertEquals(2, dogs.size());
                    }
                };
                dogs.addChangeListener(listenerFineGrained);
                callbacks[1] = listenerFineGrained;

                new Thread() {
                    @Override
                    public void run() {
                        Realm bgRealm = Realm.getInstance(realm.getConfiguration());
                        bgRealm.beginTransaction();
                        bgRealm.createObject(Dog.class);
                        bgRealm.commitTransaction();
                        bgRealm.close();
                        bgNonLooperThread.countDown();
                    }
                }.start();

            }
        });

        TestHelper.awaitOrFail(bgNonLooperThread);
        TestHelper.awaitOrFail(signalTestFinished);
        assertEquals(1, fineGrainedCommitInvocations.get());
    }
    // UC 5 Async RealmResults
    public void test_non_looper_thread_commit_realmresults_async () {
        final CountDownLatch bgNonLooperThread = new CountDownLatch(1);
        // Keep strong reference to the callback
        // TODO remove this once the PR about strong reference is merged
        final RealmChangeListener[] callbacks = new RealmChangeListener[2];
        handler.post(new Runnable() {
            @Override
            public void run() {
                realm = Realm.getInstance(configuration);

                RealmChangeListener listener = new RealmChangeListener() {
                    @Override
                    public void onChange() {
                        if (globalCommitInvocations.incrementAndGet() == 2) {
                            signalTestFinished.countDown();
                        }
                    }
                };
                realm.addChangeListener(listener);
                callbacks[0] = listener;

                realm.beginTransaction();
                Dog akamaru = realm.createObject(Dog.class);
                akamaru.setName("Akamaru");
                realm.commitTransaction();

                final RealmResults<Dog> dogs = realm.where(Dog.class).findAllAsync();
                assertTrue(dogs.load());
                RealmChangeListener listenerFineGrained = new RealmChangeListener() {
                    @Override
                    public void onChange() {
                        fineGrainedCommitInvocations.incrementAndGet();
                        assertEquals(2, dogs.size());
                    }
                };
                dogs.addChangeListener(listenerFineGrained);
                callbacks[1] = listenerFineGrained;

                new Thread() {
                    @Override
                    public void run() {
                        Realm bgRealm = Realm.getInstance(realm.getConfiguration());
                        bgRealm.beginTransaction();
                        bgRealm.createObject(Dog.class);
                        bgRealm.commitTransaction();
                        bgRealm.close();
                        bgNonLooperThread.countDown();
                    }
                }.start();

            }
        });

        TestHelper.awaitOrFail(bgNonLooperThread);
        TestHelper.awaitOrFail(signalTestFinished);
        assertEquals(1, fineGrainedCommitInvocations.get());
    }

    // ********************************************************************************* //
    // UC 6.
    // Callback should be invoked when the same Looper thread commit
    // ********************************************************************************* //

    // UC 5 for Sync RealmObject
    // UC 5 Async RealmObject
    // UC 5 Sync RealmResults
    // UC 5 Async RealmResults

    // ********************************************************************************* //
    // UC 7.
    // Callback should throw if registered on a non Looper thread (should be done on Strong Reference PR
    // ********************************************************************************* //

    // UC 5 for Sync RealmObject
    public void test_should_throw_on_non_looper_thread_realmobject_sync () {
        // TODO remove when callbacks becomes strong reference
        final RealmChangeListener[] callbacks = new RealmChangeListener[2];
        new Thread() {
            @Override
            public void run() {
                Realm bgRealm = Realm.getInstance(realm.getConfiguration());
                callbacks[0] =  new RealmChangeListener() {
                    @Override
                    public void onChange() {
                        fail("Callback should not be registered and invoked on a non-Looper thread");
                    }
                };
                bgRealm.addChangeListener(callbacks[0]);
                bgRealm.beginTransaction();
                bgRealm.createObject(Dog.class);
                bgRealm.commitTransaction();
                bgRealm.close();
                signalTestFinished.countDown();
            }
        }.start();
        TestHelper.awaitOrFail(signalTestFinished);
    }
    // UC 5 Async RealmObject
    // UC 5 Sync RealmResults
    // UC 5 Async RealmResults

}
