import React, {Component} from 'react'
import './App.css';
import * as swim from "swim-wasm-chat";

export default class App extends Component {

    constructor(props) {
        super(props);
        this.state = {form_message: ''};

        this.handleChange = this.handleChange.bind(this);
        this.send_message = this.send_message.bind(this);
    }

    async componentDidMount() {
        const chat_client = await new swim.ChatClient();
        chat_client.on_message(function (a,b) {
            console.log("On message callback: " + a + " " +  b);
        });

        this.setState({client: chat_client})
    }

    handleChange(event) {
        this.setState({form_message: event.target.value});
    }

    async send_message(event) {
        event.preventDefault();

        let chat_client = this.state.client;
        let msg = swim.Message.new(this.state.form_message, "test");

        await chat_client.send_message(msg).then((r) => {
            if (r !== true) {
                alert("Failed to send message");
            }
        });
    }

    render() {
        return (
            <form onSubmit={this.send_message}>
                <label>
                    Message:
                    <input type="text" value={this.state.form_message} onChange={this.handleChange}/>
                </label>
                <input type="submit" value="Submit"/>
            </form>
        );
    }

}
