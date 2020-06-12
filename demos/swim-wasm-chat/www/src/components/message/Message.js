import React from 'react';
import { Comment } from 'antd';

const Message = ({ message }) => (
    <Comment 
        author={message.userName}
        content={
            <p>
                {message.value}
            </p> 
        }
    />
);

export default Message;