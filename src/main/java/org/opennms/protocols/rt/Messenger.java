/*
 * This file is part of the OpenNMS(R) Application.
 *
 * OpenNMS(R) is Copyright (C) 2009 The OpenNMS Group, Inc.  All rights reserved.
 * OpenNMS(R) is a derivative work, containing both original code, included code and modified
 * code that was published under the GNU General Public License. Copyrights for modified
 * and included code are below.
 *
 * OpenNMS(R) is a registered trademark of The OpenNMS Group, Inc.
 *
 * Original code base Copyright (C) 1999-2001 Oculan Corp.  All rights reserved.
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
 * OpenNMS Licensing       <license@opennms.org>
 *     http://www.opennms.org/
 *     http://www.opennms.com/
 */
package org.opennms.protocols.rt;

import java.io.IOException;
import java.util.Queue;


/**
 * A class that represents a simple messaging interface.  This is possibly loss less and does not imply 
 * guaranteed deliver.  The only real requirements are those implied by the Request and Reply interfaces
 *
 * @author brozow
 */
public interface Messenger<ReqT, ReplyT> {
    
    /**
     * Send a message using the messenger service
     */
    public void sendRequest(ReqT request) throws IOException ;
    
    /**
     * Start listening for replies and enqueue any replies received to the passed in queue
     * These replies will be pulled off by users of the service and processed.
     */
    public void start(Queue<ReplyT> replyQueue);
    

}
