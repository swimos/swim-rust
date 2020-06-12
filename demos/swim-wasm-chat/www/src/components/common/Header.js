import React, {Component} from 'react';
import {Link, withRouter} from 'react-router-dom';
import './Header.css';
import {Dropdown, Icon, Layout, Menu, notification} from 'antd';

const Header = Layout.Header;

class AppHeader extends Component {
    constructor(props) {
        super(props);
    }

    render() {        
        return (
            <Header className="app-header">
                <div className="container">
                    <div className="app-title">
                        <Link to="/">SWIM Rust Chat Demo</Link>
                    </div>
                </div>
            </Header>
        );
    }
}

export default withRouter(AppHeader);