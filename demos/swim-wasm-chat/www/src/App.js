import React, {Component} from 'react'
import './App.css';
import {
    Switch,
    Route,
    withRouter
} from "react-router-dom";
import {Layout} from 'antd';
import Home from './Home';
import AppHeader from './components/common/Header';
import Chat from './Chat';
import NotFound from './components/common/NotFound';
import { USER_NAME } from './components/common/Constants';

const {Content} = Layout;

class App extends Component {

    constructor(props) {
        super(props);

        this.state = {form_message: ''};
        this.handleLogin = this.handleLogin.bind(this);
        this.handleChange = this.handleChange.bind(this);
    }

    handleChange(event) {
        this.setState({form_message: event.target.value});
    }

    handleLogin(e) {
        localStorage.setItem(USER_NAME, e.username);
        this.props.history.push("/chat");
    }

    render() {
        return (
            <Layout className="app-container">
                <AppHeader/>
                
                <Content className="app-content">
                    <Switch>
                        <Route exact path="/" render={(props) => <Home onLogin={this.handleLogin} {...props} />}></Route>
                        <Route path="/chat">
                            <Chat username={this.state.username}></Chat>
                        </Route>

                        <Route component={NotFound}></Route>
                    </Switch>
                </Content>
            </Layout>
        );
    }

}

export default withRouter(App);