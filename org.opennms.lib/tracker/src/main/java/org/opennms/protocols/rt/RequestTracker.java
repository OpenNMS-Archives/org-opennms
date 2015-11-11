/*
 * This file is part of the OpenNMS(R) Application.
 *
 * OpenNMS(R) is Copyright (C) 2007 The OpenNMS Group, Inc.  All rights reserved.
 * OpenNMS(R) is a derivative work, containing both original code, included code and modified
 * code that was published under the GNU General Public License. Copyrights for modified
 * and included code are below.
 *
 * OpenNMS(R) is a registered trademark of The OpenNMS Group, Inc.
 *
 * Modifications:
 * 
 * Created January 31, 2007
 *
 * Copyright (C) 2007 The OpenNMS Group, Inc.  All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * For more information contact:
 *      OpenNMS Licensing       <license@opennms.org>
 *      http://www.opennms.org/
 *      http://www.opennms.com/
 */
package org.opennms.protocols.rt;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * Request Tracker Design
 * 
 * The request tracker has five components that are all static
 * 
 * a messenger
 * a pendingRequest map
 * a pendingReply queue (LinkedBlockingQueue)
 * a requestIdsWithPendingReplies set
 * a timeout queue (DelayQueue)
 * 
 * It also has two threads:
 * 
 * a thread to process the pendingReplyQueue - (icmp reply processor)
 * a thread to process the timeouts (icmp timeout processor)
 * 
 * Processing:
 * 
 * All requests are asynchronous (if synchronous requests are need that
 * are implemented using asynchronous requests and blocking callbacks)
 * 
 * Making a request: (client thread)
 * - create a request (client does this) 
 * - add it to a pendingRequestMap
 * - send the request (via the Messenger)
 * - add it to the timeout queue
 * 
 * Replies come from the messenger: 
 * - the messenger is 'started' by passing in a reference to a callback
 * - as replies come in, the callback is issued
 * - the callback add the replies to the pendingReply queue, and if
 *   the replies have associated request ids, the request ids are added to the
 *   requestIdsWithPendingReplies set
 *
 * Processing a reply: (reply processor)
 * - take a reply from the pendingReply queue
 * - look up and remove the matching request in the pendingRequest map
 * - call request.processReply(reply) - this will store the reply and
 *   call the handleReply call back
 * - pending request sets completed to true
 * - removes the request id from the requestIdsWithPendingReplies set
 * 
 * Processing a timeout:
 * - take a request from the timeout queue
 * - if the request is completed discard it
 * - if requestIdsWithPendingReplies contains the request id, meaning we've
 *   received a reply but haven't processed it yet, discard the timeout
 * - otherwise, call request.processTimeout(), this will check the number
 *   of retries and either return a new request with fewer retries or
 *   call the handleTimeout call back
 * - if processTimeout returns a new request than process it as in Making
 *   a request 
 * 
 * Thread Details:
 * 
 * 1.  The reply processor that will pull replies off the linked
 *     blocking queue and process them.  This will result in calling the
 *     PingResponseCallback handleReply method.
 * 
 * 2.  The timeout processor that will pull PingRequests off of a
 *     DelayQueue.  A DelayQueue does not allow things to be removed from
 *     them until the timeout has expired.
 * 
 */

/**
 * A class for tracking sending and received of arbitrary messages. The
 * transport mechanism is irrelevant and is encapsulated in the Messenger
 * request. Timeouts and Retries are handled by this mechanism and provided to
 * the request object so they can be processed. A request is guaranteed to
 * have one of its process method called no matter what happens. This makes it
 * easier to write code because some kind of indication is always provided and
 * so timing out is not needed in the client.
 * 
 * @author <a href="mailto:brozow@opennms.org">Mathew Brozowski</a>
 */
public class RequestTracker<ReqT extends Request<?, ReqT, ReplyT>, ReplyT extends Response> implements ReplyHandler<ReplyT> {
    
    private static final Logger s_log = LoggerFactory.getLogger(RequestTracker.class);
    
    private RequestLocator<ReqT, ReplyT> m_requestLocator;
    private Messenger<ReqT, ReplyT> m_messenger;
    private final Map<Object, Boolean> m_requestIdsWithPendingReplies;
    private BlockingQueue<ReplyT> m_pendingReplyQueue;
    private DelayQueue<ReqT> m_timeoutQueue;

    private Thread m_replyProcessor;
    private Thread m_timeoutProcessor;
    
    private static final int NEW = 0;
    private static final int STARTING = 1;
    private static final int STARTED = 2;
    
    private AtomicInteger m_state = new AtomicInteger(NEW);

	/**
     * Construct a RequestTracker that sends and received messages using the
     * indicated messenger. The name is using to name the threads created by
     * the tracker.
     */
    public RequestTracker(String name, Messenger<ReqT, ReplyT> messenger, RequestLocator<ReqT, ReplyT> requestLocator) throws IOException {
        
        m_requestLocator = requestLocator;
	    m_pendingReplyQueue = new LinkedBlockingQueue<ReplyT>();
	    m_requestIdsWithPendingReplies = new ConcurrentHashMap<Object, Boolean>();
	    m_timeoutQueue = new DelayQueue<ReqT>();

	    m_replyProcessor = new Thread(name+"-Reply-Processor") {
	        public void run() {
	            try {
	                processReplies();
	            } catch (InterruptedException e) {
                    s_log.error("Thread {} interrupted!", this);
	            } catch (Throwable t) {
                    s_log.error("Unexpected exception on Thread " + this + "!", t);
	            }
	        }
	    };
	    
	    m_timeoutProcessor = new Thread(name+"-Timeout-Processor") {
	        public void run() {
	            try {
	                processTimeouts();
	            } catch (InterruptedException e) {
                    s_log.error("Thread {} interrupted!", this);
	            } catch (Throwable t) {
                    s_log.error("Unexpected exception on Thread " + this + "!", t);
	            }
	        }
	    };
	    
        m_messenger = messenger;

	}
    
    /**
     * This method starts all the threads that are used to process the
     * messages and also starts the messenger.
     */
    public synchronized void start() {
        boolean startNeeded = m_state.compareAndSet(NEW, STARTING);
        if (startNeeded) {
            m_messenger.start(this);
            m_timeoutProcessor.start();
            m_replyProcessor.start();
            m_state.set(STARTED);
        }
    }
    
    public void assertStarted() {
        boolean started = m_state.get() == STARTED;
        if (!started) throw new IllegalStateException("RequestTracker not started!");
    }

    /**
     * Send a tracked request via the messenger. The request is tracked for
     * timeouts and retries. Retries are sent if the timeout processing
     * indicates that they should be.
     */
    public void sendRequest(ReqT request) throws Exception {
        assertStarted();
        if (!m_requestLocator.trackRequest(request)) return;
        m_messenger.sendRequest(request);
        s_log.debug("Scheding timeout for request to {} in {} ms", request, request.getDelay(TimeUnit.MILLISECONDS));
        m_timeoutQueue.offer(request);
    }

    public void handleReply(ReplyT reply) {
        m_pendingReplyQueue.offer(reply);

        // Only track the request id, if the reply supports it
        if (reply instanceof ResponseWithId) {
            m_requestIdsWithPendingReplies.put(((ResponseWithId<?>) reply).getRequestId(), Boolean.TRUE);
        }
    }

    private void processReplies() throws InterruptedException {
	    while (true) {
	        ReplyT reply = m_pendingReplyQueue.take();
            s_log.debug("Found a reply to process: {}", reply);
            
            ReqT request = locateMatchingRequest(reply);

            if (request != null) {
	            if (processReply(reply, request)) {
	                m_requestLocator.requestComplete(request);
	            }

	            m_requestIdsWithPendingReplies.remove(request.getId());
	        } else {
	            s_log.debug("No request found for reply {}", reply);
	        }
	    }
    }

    private ReqT locateMatchingRequest(ReplyT reply) {
        try {
            return m_requestLocator.locateMatchingRequest(reply);
        } catch (Throwable t) {
            s_log.error("Unexpected error locating response to request " + reply + ". Discarding response!", t);
            return null;
        }
    }

    private boolean processReply(ReplyT reply, ReqT request) {
        try {
            s_log.debug("Processing reply {} for request {}", reply, request);
            return request.processResponse(reply);
        } catch (Throwable t) {
            s_log.error("Unexpected error processingResponse to request: " + request + ", reply is " + reply, t);
            // we should throw away the request if this happens
            return true;
        }
    }

	private void processTimeouts() throws InterruptedException {  
	    while (true) {
	        try {
	            ReqT timedOutRequest = m_timeoutQueue.take();
	            processNextTimeout(timedOutRequest);
	        } catch (Throwable t) {
	            s_log.error("Unexpected error processingTimeout!", t);
	        }
	    }
	}

	private void processNextTimeout(ReqT timedOutRequest) {

        // do nothing is the request has already been processed.
        if (timedOutRequest.isProcessed()) {
            return;
        }

        s_log.debug("Found a possibly timed-out request: {}", timedOutRequest);
        ReqT pendingRequest = m_requestLocator.requestTimedOut(timedOutRequest);

        if (pendingRequest == timedOutRequest) {
            // the request is still pending

            // is there a pending reply that we haven't processed yet?
            if (m_requestIdsWithPendingReplies.containsKey(timedOutRequest.getId())) {
                // There is a reply pending in the reply queue, but we haven't
                // had a chance to process it yet. Wait for the Reply Processor thread
                // to process the response instead of the processing the timeout
                s_log.info("A timeout was issued while the reply is pending processing for: {}", timedOutRequest);
                return;
            }

            // we must time it out
            ReqT retry = processTimeout(timedOutRequest);
            if (retry != null) {
                try {
                    sendRequest(retry);
                } catch (Exception e) {
                    retry.processError(e);
                }
            }
        } else if (pendingRequest != null) {
            String msg = String.format("A pending request %s with the same id exists but is not the timeout request %s from the queue!", pendingRequest, timedOutRequest);
            s_log.error(msg);
            timedOutRequest.processError(new IllegalStateException(msg));
        }
	}

    private ReqT processTimeout(ReqT request) {
        try {
            s_log.debug("Processing timeout for: {}", request);
            return request.processTimeout();
        } catch (Throwable t) {
            s_log.error("Unexpected error processingTimout to request: " + request, t);
            return null;
        }
    }

}
