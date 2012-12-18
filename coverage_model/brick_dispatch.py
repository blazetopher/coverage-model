#!/usr/bin/env python

"""
@package coverage_model.brick_dispatch
@file coverage_model/brick_dispatch.py
@author Christopher Mueller
@brief Module containing classes for delegating the writing of values to persistence bricks using a pool of writer processes
"""

import os

from pyon.util.async import spawn
from coverage_model.utils import create_guid
from gevent.event import AsyncResult

from ooi.logging import log
from gevent_zeromq import zmq
from zmq.core.error import ZMQError
from gevent import queue
import time
import random
from pyon.core.interceptor.encode import encode_ion, decode_ion
from msgpack import packb, unpackb
import numpy as np

REQUEST_WORK = 'REQUEST_WORK'
SUCCESS = 'SUCCESS'
FAILURE = 'FAILURE'
PORT_RANGE = [10000,20000]
WORK_FAILURE_RETRIES = 4

def pack(msg):
    return packb(msg, default=encode_ion)

def unpack(msg):
    return unpackb(msg, object_hook=decode_ion)

class BreakError(Exception):
    pass

class BaseBrickWriterDispatcher(object):

    def __init__(self, failure_callback, num_workers=1):
        self.guid = create_guid()
        self.prep_queue = queue.Queue()
        self.work_queue = queue.Queue()
        self._pending_work = {}
        self._stashed_work = {}
        self._active_work = {}
        self._failures = {}
        self._do_stop = False
        self._count = -1
        self._is_shutdown = False
        self._failure_callback = failure_callback

        self.num_workers = num_workers if num_workers > 0 else 1
        self.is_single_worker = self.num_workers == 1
        self.workers = []

        self._setup()
        self._configure_workers()

    def has_pending_work(self):
        return len(self._pending_work) > 0

    def has_active_work(self):
        return len(self._active_work) > 0

    def has_stashed_work(self):
        return len(self._stashed_work) > 0

    def is_dirty(self):
        if not self.has_active_work():
            if not self.has_stashed_work():
                if not self.has_pending_work():
                    return False

        return True

    def get_dirty_values_async_result(self):
        dirty_async_res = AsyncResult()
        def dirty_check(self, res):
            while True:
                if self.is_dirty():
                    time.sleep(0.1)
                else:
                    res.set(True)
                    break

        spawn(dirty_check, self, dirty_async_res)

        return dirty_async_res

    def run(self):
        self._do_stop = False
        self._org_g = spawn(self._work_organizer)

        self._prov_g = spawn(self._work_provisioner)
        self._rec_g = spawn(self._work_result_receiver)

    def shutdown(self, force=False, timeout=None):
        if self._is_shutdown:
            return
            # CBM TODO: Revisit to ensure this won't strand work or terminate workers before they complete their work...!!
        self._do_stop = True
        try:
            log.debug('Shutdown:  force == %s', force)
            if not force:
                log.debug('Waiting for organizer; timeout == %s',timeout)
                # Wait for the organizer to finish - ensures the prep_queue is empty
                self._org_g.join(timeout=timeout)

                log.debug('Waiting for provisioner; timeout == %s',timeout)
                # Wait for the provisioner to finish - ensures work_queue is empty
                self._prov_g.join(timeout=timeout)

                log.debug('Waiting for receiver; timeout == %s',timeout)
                # Wait for the receiver to finish - allows workers to finish their work
                self._rec_g.join(timeout=timeout)

            log.debug('Killing organizer, provisioner, and receiver greenlets')
            # Terminate the greenlets
            self._org_g.kill()
            self._prov_g.kill()
            self._rec_g.kill()
            log.debug('Greenlets killed')

            log.debug('Shutdown workers')
            # Shutdown workers - work should be completed by now...
            for worker in self.workers:
                worker.stop()

            log.debug('Workers shutdown')
        except:
            raise
        finally:
            self._shutdown()
            self._is_shutdown = True

    def put_work(self, work_key, work_metrics, work):
        if self._is_shutdown:
            raise SystemError('This BrickDispatcher has been shutdown and cannot process more work!')
        self.prep_queue.put((work_key, work_metrics, work))

    def _work_organizer(self):
        while True:
            if self._do_stop and self.prep_queue.empty():
                break
            try:
                # Timeout after 1 second to allow stopage and _stashed_work cleanup
                wd = self.prep_queue.get(timeout=1)
            except queue.Empty:
                # No new work added - see if there's anything on the stash to cleanup...
                for k in self._stashed_work:
                    log.debug('Cleanup _stashed_work...')
                    # Just want to trigger cleanup of the _stashed_work, pass an empty list of 'work', gets discarded
                    self.put_work(k, self._stashed_work[k][0], [])
                continue

            try:
                k, wm, w = wd
                is_list = isinstance(w, list)
                if k not in self._stashed_work and len(w) == 0:
                    log.debug('Discarding empty work')
                    continue

#                log.debug('Work: %s',w)

                if k in self._active_work:
                    log.debug('Do Stash')
                    # The work_key is being worked on
                    if k not in self._stashed_work:
                        # Create the stash for this work_key
                        self._stashed_work[k] = (wm, [])

                    # Add the work to the stash
                    if is_list:
                        self._stashed_work[k][1].extend(w[:])
                    else:
                        self._stashed_work[k][1].append(w)
                else:
                    # If there is a stash for this work_key, prepend it to work
                    if k in self._stashed_work:
                        log.debug('Stashed work exists, prepending it to the current work')
                        # NOTE: Only uncomment the following log statement when debugging interactively - may print lots of information!!
#                        log.debug('Was a stash, prepend: %s, %s', self._stashed_work[k], w)
                        _, sv=self._stashed_work.pop(k)
                        if is_list:
                            sv.extend(w[:])
                        else:
                            sv.append(w)
                        w = sv
                        is_list = True # Work is a list going forward!!

#                    log.debug('Work: %s',w)

                    # The work_key is not yet pending
                    not_in_pend = k not in self._pending_work

                    if not_in_pend:
                        # Create the pending for this work_key
                        log.debug('-> new pointer \'%s\'', k)
                        self._pending_work[k] = (wm, [])

                    # Add the work to the pending
                    log.debug('-> adding work to \'%s\'', k)
                    # NOTE: Only uncomment the following log statement when debugging interactively - may print lots of information!!
#                    log.debug('-> adding work to \'%s\': %s', k, w)
                    if is_list:
                        self._pending_work[k][1].extend(w[:])
                    else:
                        self._pending_work[k][1].append(w)

                    if not_in_pend:
                        # Add the not-yet-pending work to the work_queue
                        self.work_queue.put(k)
            except:
                raise

    def _work_provisioner(self):
        while True:
            try:
                if self._do_stop and self.work_queue.empty():
                    break
                log.debug('Receive work request (loop)')

                try:
                    msg = self.receive_work_request()
                except BreakError:
                    break

                if msg is not None:
                    _, worker_guid = unpack(msg)
                    log.debug('Get work from work_queue (loop)')
                    work_key = None
                    while work_key is None:
                        try:
                            work_key = self.work_queue.get(block=False)
                        except queue.Empty:
                            if self._do_stop:
                                break
                            else:
                                time.sleep(0.1)

                    if work_key is not None:
                        log.debug('Preparing work key %s', work_key)
                        work_metrics, work = self._pending_work.pop(work_key)

                        wp = (work_key, work_metrics, work)
                        log.debug('Assigning work to worker \'%s\': work_key==%s', worker_guid, work_key)
                        # NOTE: Only uncomment the following log statement when debugging interactively - may print lots of information!!
#                        log.debug('Assigning work to worker \'%s\': work==%s', worker_guid, work)
                        pw = pack(wp)

                        self._active_work[work_key] = (worker_guid, pw)
                        self.send_work(pw)
            finally:
            #       e         time.sleep(0.1)
                pass

    def _work_result_receiver(self):
        while True:
            try:
                if self._do_stop and len(self._active_work) == 0:
                    break

                log.debug('Receive response message (loop)')

                try:
                    msg = self.receive_work_result()
                except BreakError:
                    break

                if msg is not None:
                    resp_type, worker_guid, work_key, work = unpack(msg)
                    work = list(work) if work is not None else work
                    if resp_type == SUCCESS:
                        log.debug('Worker %s was successful', worker_guid)
                        wguid, pw = self._active_work.pop(work_key)
                        if pw in self._failures:
                            self._failures.pop(pw)
                    elif resp_type == FAILURE:
                        log.debug('Failure reported for work on %s by worker %s', work_key, worker_guid)
                        if work_key is None:
                            # Worker failed before it did anything, put all work back on the prep queue to be reorganized by the organizer
                            # Because it failed so miserably, need to find the work_key based on guid
                            for k, v in self._active_work.iteritems():
                                if v[0] == worker_guid:
                                    work_key = k
                                    break

                            if work_key is not None:
                                wguid, pw = self._active_work.pop(work_key)
                                try:
                                    self._add_failure(pw)
                                except ValueError,e:
                                    self._failure_callback(e.message, unpack(pw))
                                    continue

                                self.put_work(*unpack(pw))
                        else:
                            # Normal failure
                            # Pop the work from active work, and queue the work returned by the worker
                            wguid, pw = self._active_work.pop(work_key)
                            try:
                                self._add_failure(pw)
                            except ValueError,e:
                                self._failure_callback(e.message, unpack(pw))
                                continue
                            _, wm, wk = unpack(pw)
                            self.put_work(work_key, wm, work)
            finally:
            #                time.sleep(0.1)
                pass

    def _add_failure(self, wp):
        pwp = pack(wp)
        log.warn('Adding to _failures: %s', pwp)
        if pwp in self._failures:
            self._failures[pwp] += 1
        else:
            self._failures[pwp] = 1

        if self._failures[pwp] > WORK_FAILURE_RETRIES:
            raise ValueError('Maximum failure retries exceeded')

    #############################################################
    ## Unimplemented functions to be overridden by sub-classes ##
    #############################################################

    def _setup(self):
        raise NotImplementedError('Not implemented in base class')

    def _configure_workers(self):
        raise NotImplementedError('Not implemented in base class')

    def _shutdown(self):
        raise NotImplementedError('Not implemented in base class')

    def receive_work_request(self):
        raise NotImplementedError('Not implemented in base class')

    def send_work(self, msg):
        raise NotImplementedError('Not implemented in base class')

    def receive_work_result(self):
        raise NotImplementedError('Not implemented in base class')


class BrickWriterDispatcher(BaseBrickWriterDispatcher):

    def __init__(self, failure_callback, num_workers=1):
        BaseBrickWriterDispatcher.__init__(self, failure_callback=failure_callback, num_workers=num_workers)

    def _setup(self):
        self.context = zmq.Context(1)
        self.prov_sock = self.context.socket(zmq.REP)
        self.prov_port = self._get_port(self.prov_sock)
        log.info('Provisioning url: tcp://*:{0}'.format(self.prov_port))

        self.resp_sock = self.context.socket(zmq.SUB)
        self.resp_port = self._get_port(self.resp_sock)
        self.resp_sock.setsockopt(zmq.SUBSCRIBE, '')
        log.info('Response url: tcp://*:{0}'.format(self.resp_port))

    def _get_port(self, socket):
        for x in xrange(PORT_RANGE[0], PORT_RANGE[1]):
            try:
                socket.bind('tcp://*:{0}'.format(x))
                return x
            except ZMQError:
                continue

    def _configure_workers(self):
        from brick_worker import run_zmq_worker
        for x in xrange(self.num_workers):
            worker = run_zmq_worker(self.context, self.prov_port, self.resp_port)
            self.workers.append(worker)

    def _shutdown(self):
        log.debug('Closing provisioner and receiver sockets')
        # Close sockets
        self.prov_sock.close()
        self.resp_sock.close()
        log.debug('Sockets closed')
        log.debug('Terminating the context')
        self.context.term()
        log.debug('Context terminated')

    def receive_work_request(self):
        if self.prov_sock.closed:
            raise BreakError()

        msg = None
        while msg is None:
            try:
                msg = self.prov_sock.recv(zmq.NOBLOCK)
            except zmq.ZMQError, e:
                if e.errno == zmq.EAGAIN:
                    if self._do_stop:
                        break
                    else:
                        time.sleep(0.1)
                else:
                    raise

        return msg

    def send_work(self, msg):
        self.prov_sock.send(msg)

    def receive_work_result(self):
        if self.resp_sock.closed:
            raise BreakError()

        msg = None
        while msg is None:
            try:
                msg = self.resp_sock.recv(zmq.NOBLOCK)
            except zmq.ZMQError, e:
                if e.errno == zmq.EAGAIN:
                    if self._do_stop:
                        break
                    else:
                        time.sleep(0.1)
                else:
                    raise

        return msg

def run_test_dispatcher(work_count, num_workers=1):
    # Set up temporary directories to save data
    import shutil
    import tempfile
    BASE_DIR = tempfile.mkdtemp()

    WORK_KEYS = ['a','b','c','d','e']

    for x in [x for x in os.listdir(BASE_DIR) if x.endswith('.h5')]:
        os.remove(os.path.join(BASE_DIR,x))

    fps = {}
    for k in WORK_KEYS:
        fps[k] = os.path.join(BASE_DIR, '{0}.h5'.format(k))
#        with h5py.File(fps[k], 'a'):
#            pass

    bD = (50,)
    cD = (5,)
    fv = -9999
    dtype = 'f'
    
    def fcb(message, work):
        log.error('WORK DISCARDED!!!; %s: %s', message, work)

    disp = BrickWriterDispatcher(fcb, num_workers=num_workers)
    disp.run()

    def make_work():
        for x in xrange(work_count):
            bk = random.choice(WORK_KEYS)
            brick_metrics = (fps[bk], bD, cD, dtype, fv)
            if np.random.random_sample(1)[0] > 0.5:
                sl = int(np.random.randint(0,10,1)[0])
                w = np.random.random_sample(1)[0]
            else:
                strt = int(np.random.randint(0,bD[0] - 2,1)[0])
                stp = int(np.random.randint(strt+1,bD[0],1)[0])
                sl = slice(strt, stp)
                w = np.random.random_sample(stp-strt)
            disp.put_work(work_key=bk, work_metrics=brick_metrics, work=([sl], w))
            time.sleep(0.1)

    spawn(make_work)

    # Remove temporary directories
    shutil.rmtree(BASE_DIR)

    return disp

"""
from coverage_model.brick_dispatch import run_test_dispatcher;
disp=run_test_dispatcher(20)

"""

"""
https://github.com/nimbusproject/pidantic
https://github.com/nimbusproject/pidantic/blob/master/pidantic/nosetests/piddler_supd_basic_test.py
https://github.com/nimbusproject/epuharness/blob/master/epuharness/harness.py
https://github.com/nimbusproject/eeagent/blob/master/eeagent/execute.py#L265
https://github.com/nimbusproject/eeagent/blob/master/eeagent/execute.py#L290
"""