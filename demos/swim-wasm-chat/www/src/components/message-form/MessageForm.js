import React from 'react';
import {Button, Form, Input, Layout} from 'antd';

const {Content} = Layout;

export default function MessageForm({onFinish}) {
    const [form] = Form.useForm();

    const submitForm = ({message}) => {
        onFinish(message);
        form.resetFields();
    };

    return (
        <Layout className="layout">
            <Content style={{padding: '50px 100px'}}>
                <Form form={form} onFinish={submitForm}>
                    <Form.Item
                        name="message"
                        rules={[{
                            required: true,
                            message: 'Please write a message',
                        }]}>
                        <Input/>
                    </Form.Item>

                    <div style={{
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                    }}>
                        <Form.Item>
                            <Button type="primary" htmlType="submit">
                                Send
                            </Button>
                        </Form.Item>
                    </div>
                </Form>
            </Content>
        </Layout>
    );
}