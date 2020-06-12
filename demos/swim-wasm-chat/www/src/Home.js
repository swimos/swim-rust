import React, {Component} from 'react'
import {Form, Input, Button, Layout} from 'antd';
import _default from 'antd/lib/time-picker';
import { UserOutlined, LockOutlined } from '@ant-design/icons';

const { Content } = Layout;

export default class Home extends Component {

    constructor(props) {
        super(props);
        this.state = {};

        this.handleSubmit = this.handleSubmit.bind(this);
    }

    handleSubmit(username) {
      this.props.onLogin(username);
    }

    render() {
        return (
          <Layout className="layout">
            <Content style={{ padding: '50px 100px' }}>
              <Form onFinish={this.handleSubmit} className="login-form">

                <Form.Item
                    name="username"
                    rules={[
                      {
                        required: true,
                        message: 'Please enter a username.',
                      },
                    ]}>
                  <Input prefix={<UserOutlined className="site-form-item-icon" />} placeholder="Username" />
                </Form.Item>

                <Form.Item>
                  <Button type="primary" htmlType="submit" className="login-form-button">
                    Log in
                  </Button>
                </Form.Item>

              </Form>
            </Content>
          </Layout>
        );
    }

}
