import React, {Component} from 'react'
import {withRouter} from 'react-router-dom'
import {Layout} from 'antd'
import {LoadingOutlined} from '@ant-design/icons';
import './App.css'
import {Form} from '@ant-design/compatible';
import MessageList from './components/message-list/MessageList';
import MessageForm from './components/message-form/MessageForm';
import {USER_NAME} from './components/common/Constants';
import * as swim from "swim-wasm-chat";
import {v4 as uuidv4} from 'uuid';

class Chat extends Component {

    constructor(props) {
        super(props)

        this.state = {
            isLoading: false
        };

        this.handleFinish = this.handleFinish.bind(this);
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
            // On initial sync
            (msgs) => {
                this.setState({
                    messages: msgs,
                    isLoading: false
                });
            },
            // On insert
            (msg) => {
                var messages = this.state.messages.concat(msg);
                this.setState({messages: messages})
            }
        );

        this.setState({client: chat_client})
    }

    async handleFinish(message) {
        let username = localStorage.getItem(USER_NAME);
        let msg = swim.Message.new(message, username, uuidv4());
        let chat_client = this.state.client;

        await chat_client.send_message(msg).then((r) => {
            if (!r) {
                alert("Failed to send message");
            }
        });
    }

    render() {
        if (this.state.isLoading) {
            return <LoadingOutlined/>
        }

        const messages = this.state.messages;

        return (
            <Layout className="layout">
                <MessageList messages={messages}/>
                <MessageForm onFinish={this.handleFinish}/>
            </Layout>
        );
    }

}

export default withRouter(Form.create()(Chat))