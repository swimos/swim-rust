import React, { Component } from 'react';
import Message from '../message/Message';
import './MessageList.css';
import { Layout } from 'antd';
const { Content } = Layout;

export default class MessageList extends Component {

    render () {
        const { messages } = this.props;

        return ( 
            <Layout>
                <Content style={{ padding: '20px 50px' }}>
                    {messages.reverse().map((message) => {
                        console.log("msg: %O", message);

                        return (
                            <div key={message.uuid}>
                                <Message message={message} />
                            </div>
                        );
                    })}
                </Content>
            </Layout>
      )
    }

}