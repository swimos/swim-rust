import React, { Component } from 'react';
import './MessageList.css';
import { List } from 'antd';
import { animateScroll } from "react-scroll";

export default class MessageList extends Component {

    constructor(props) {
        super(props)

        this.state = {
            loading: false,
            hasMore: true,
        };

        this.onChange = this.onChange.bind(this);
    }

    onChange() {
        alert("Changed")
    }

    scrollToBottom() {
        animateScroll.scrollToBottom({
          containerId: "container"
        });
    }

    componentDidMount() {
        this.scrollToBottom();
    }

    componentDidUpdate() {
        this.scrollToBottom();
    }

    render () {
        const { messages } = this.props;

        return ( 
            <div id="container" className="messageListContainer">
               <List
                itemLayout="horizontal"
                dataSource={messages}
                renderItem={message => (
                    <List.Item>
                        <List.Item.Meta
                        title={message.userName}
                        description={message.value}
                        />
                    </List.Item>)}
                />
            </div>
            
      )
    }

}