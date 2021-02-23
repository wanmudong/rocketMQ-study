/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.common.message;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.apache.rocketmq.common.MixAll;

public class MessageBatch extends Message implements Iterable<Message> {

    private static final long serialVersionUID = 621335151046335557L;
    private final List<Message> messages;

    private MessageBatch(List<Message> messages) {
        this.messages = messages;
    }

    public byte[] encode() {
        return MessageDecoder.encodeMessages(messages);
    }

    public Iterator<Message> iterator() {
        return messages.iterator();
    }

    /**
     * MessageBatch#generateFromList
     * @param messages
     * @return
     */
    public static MessageBatch generateFromList(Collection<Message> messages) {
        assert messages != null;
        assert messages.size() > 0;
        List<Message> messageList = new ArrayList<Message>(messages.size());
        Message first = null;
        for (Message message : messages) {
            //判断延时级别，如果大于0抛出异常，原因为：批量消息发送不支持延时
            if (message.getDelayTimeLevel() > 0) {
                throw new UnsupportedOperationException("TimeDelayLevel is not supported for batching");
            }
            // 判断topic是否以 **"%RETRY%"** 开头，如果是，
            // 则抛出异常，原因为：批量发送消息不支持消息重试
            if (message.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                throw new UnsupportedOperationException("Retry Group is not supported for batching");
            }
            // 判断集合中的每个Message的topic与批量发送topic是否一致，
            // 如果不一致则抛出异常，原因为：
            // 批量消息中的每个消息实体的Topic要和批量消息整体的topic保持一致。
            if (first == null) {
                first = message;
            } else {
                if (!first.getTopic().equals(message.getTopic())) {
                    throw new UnsupportedOperationException("The topic of the messages in one batch should be the same");
                }
                // 判断批量消息的首个Message与其他的每个Message实体的等待消息存储状态是否相同，
                // 如果不同则报错，原因为：批量消息中每个消息的waitStoreMsgOK状态均应该相同。
                if (first.isWaitStoreMsgOK() != message.isWaitStoreMsgOK()) {
                    throw new UnsupportedOperationException("The waitStoreMsgOK of the messages in one batch should the same");
                }
            }
            messageList.add(message);
        }
        MessageBatch messageBatch = new MessageBatch(messageList);

        messageBatch.setTopic(first.getTopic());
        messageBatch.setWaitStoreMsgOK(first.isWaitStoreMsgOK());
        return messageBatch;
    }

}
