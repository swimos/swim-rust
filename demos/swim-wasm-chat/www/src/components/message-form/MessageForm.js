import React from 'react';
import { Form, Input, Button } from 'antd';

const FormItem = Form.Item;

const MessageForm = ( { onFinish }) => {
    return (
        <Form onFinish={onFinish} className="messageBox">
            <FormItem
                name="message"
                rules={[
                {
                    required: true,
                    message: 'Please input your message!',
                },
                ]}>
                <Input placeholder="Message..." />
            </FormItem>

            <FormItem>
                <Button type="primary" htmlType="submit" className="messagebox">
                    Send Message
                </Button>
            </FormItem>
        </Form>
    )
}

export default MessageForm;