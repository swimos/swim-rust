import React, { Component, useEffect } from 'react'
import { withRouter } from 'react-router-dom'
import { Layout } from 'antd'
import { LoadingOutlined } from '@ant-design/icons';
import './App.css'
import { Form } from '@ant-design/compatible';
import MessageList from './components/message-list/MessageList';
import MessageForm from './components/message-form/MessageForm';
import { USER_NAME } from './components/common/Constants';
import * as swim from "swim-wasm-chat";

class Chat extends Component {

    constructor(props) {
        super(props)
        
        this.state = {
            isLoading:false
        };

        this.sendMessage = this.sendMessage.bind(this);
    }

    async componentWillMount() {
        this.setState({
            isLoading: true
        });

       await this.loadChats();
    }

    async loadChats() {
        const chat_client = await new swim.ChatClient();

        chat_client.set_callbacks(
            (msgs) => {                
                this.setState({
                    messages:msgs,
                    isLoading:false
                });
            }, 
            (msg) => {
                this.state.messages.push(msg);
                console.log("Received message: %O", msg);
            }
        );

        this.setState({client: chat_client})
    }

    async sendMessage(form) {
        let username = localStorage.getItem(USER_NAME);
        console.log("Sending message. Username: %O, message: %O", username, form.message);

        let chat_client = this.state.client;
        let msg = swim.Message.new(form.message, username, "a", "b");

        await chat_client.send_message(msg).then((r) => {
            if (r !== true) {
                alert("Failed to send message");
            }
        });
    }

    render() {
        if (this.state.isLoading) {
            return <LoadingOutlined />
        }

        const messages = this.state.messages;

        return (
            <Layout className="layout">
                <MessageList messages={messages}/>
                <MessageForm onFinish={this.sendMessage}/>
            </Layout>
        );
    }

}

export default withRouter(Form.create()(Chat))