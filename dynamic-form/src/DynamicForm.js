import React, { useState, useEffect } from "react";
import dayjs from "dayjs";
import { UserOutlined, LoadingOutlined, PlusOutlined } from "@ant-design/icons";
import {
  Button,
  Form,
  Input,
  Collapse,
  Avatar,
  Space,
  Tabs,
  Upload,
  Checkbox,
  Radio,
} from "antd";
import { message, Row, Col } from "antd";
import { UploadOutlined } from "@ant-design/icons";
import FormDate from "./form/FormDate";
import axios from "axios";

const getBase64 = (img, callback) => {
  const reader = new FileReader();
  reader.addEventListener("load", () => callback(reader.result));
  reader.readAsDataURL(img);
};
const beforeUpload = (file) => {
  const isJpgOrPng = file.type === "image/jpeg" || file.type === "image/png";
  if (!isJpgOrPng) {
    message.error("You can only upload JPG/PNG file!");
  }
  const isLt2M = file.size / 1024 / 1024 < 2;
  if (!isLt2M) {
    message.error("Image must smaller than 2MB!");
  }
  return isJpgOrPng && isLt2M;
};

const DynamicForm = ({ data }) => {
  const [items, setItems] = useState([]);
  const [form] = Form.useForm();
  const [current, setCurrent] = useState(0);

  const formConfig = data;
  const [formData, setFormData] = useState({});
  const [imageUrl, setImageUrl] = useState();
  const [loading, setLoading] = useState(false);

  const handleChange = (date, string) => {
    let { type, label } = date;
    if (type === "date") {
      setFormData({ ...formData, [label]: string });
    }
  };

  const dateChange = (date, string, field) => {
    setFormData({ ...formData, [field.label]: dayjs(string) });
  };

  const getFormItem = (field, index) => {
    switch (field.type) {
      case "avatar1":
        return (
          <Upload
            name="avatar"
            listType="picture-circle"
            className="avatar-uploader"
            showUploadList={false}
            action="https://mocki.io/v1/6622d20c-38c4-424c-84d9-0fe8b9f3bb5a"
            beforeUpload={beforeUpload}
            onChange={handleChange}
          >
            {imageUrl ? (
              <img
                src={imageUrl}
                alt="avatar"
                style={{
                  width: "100%",
                }}
              />
            ) : (
              uploadButton
            )}
          </Upload>
        );
      case "text":
      case "email":
      case "password":
      case "file":
        return (
          <Form.Item
            label={field.label}
            name={field.name}
            rules={[
              {
                required: field.required ? field.required : false,
                message: `"Please input ${field.name}!"`,
              },
            ]}
            key={index}
            def
          >
            <Input />
          </Form.Item>
        );

      case "date":
        return <FormDate field={field} index={index} onChange={dateChange} />;
      case "checkbox":
        return (
          <Form.Item
            name={field.name}
            rules={[
              {
                required: field.required ? field.required : false,
                message: `"Please check ${field.name} to agree to terms & conditions"`,
              },
            ]}
            key={index}
          >
            <Checkbox checked={true} disabled={false} onChange={handleChange}>
              {field.label}
            </Checkbox>
          </Form.Item>
        );
      case "radio":
        return (
          <Form.Item
            label={field.label}
            name={field.name}
            rules={[
              {
                required: field.required ? field.required : false,
                message: `"Please select ${field.name}"`,
              },
            ]}
            key={index}
          >
            <Radio.Group>
              {field.options.map((option, optIndex) => (
                <Radio value={option.value} key={optIndex}>
                  {option.label}
                </Radio>
              ))}
            </Radio.Group>
          </Form.Item>
        );
      case "kyclist":
        return (
          <Row key={index} gutter={10}>
            <Col span="12">
              <Form.Item
                label={field.label}
                labelCol={{ flex: "180px" }}
                name={field.name}
                rules={[
                  {
                    required: field.required && false,
                    message: `"Please input ${field.name}"`,
                  },
                ]}
              >
                <Input defaultValue={field.value} />
              </Form.Item>
            </Col>
            <Col span="10">
              <Form.Item
                name={field.doc_file_name}
                rules={[
                  {
                    required: field.required && false,
                    message: `"Please upload ${field.name} document"`,
                  },
                ]}
              >
                <Upload
                  name={field.doc_file_name}
                  action="/upload.do"
                  listType="picture"
                >
                  <Button icon={<UploadOutlined />}>Click to Upload</Button>
                </Upload>
              </Form.Item>
            </Col>
          </Row>
        );
      case "list":
        return (
          <div key={index}>
            <Form.Item
              label={field.label}
              name={field.doc_file_name}
              rules={[
                {
                  required: field.required && false,
                  message: `"Please upload ${field.name} document"`,
                },
              ]}
              labelCol={{ flex: "300px" }}
              labelAlign="left"
              labelWrap={true}
              wrapperCol={{ flex: 1 }}
              style={{ maxWidth: 600 }}
            >
              <Upload
                name={field.doc_file_name}
                action="/upload.do"
                listType="picture"
              >
                <Button icon={<UploadOutlined />}>Click to Upload</Button>
              </Upload>
            </Form.Item>
          </div>
        );
      default:
        return null;
    }
  };

  const handleSubmit = async (values) => {
    console.log("Form values:", values);

    // const formFields = new FormData();
    // formData.forEach((field) => {
    //   if (field.type === "file") {
    //     formData.append(field.name, values[field.name][0].originFileObj);
    //   } else {
    //     formData.append(field.name, values[field.name]);
    //   }
    // });

    try {
      await axios.post("http://localhost:5000/api/forms/submit", values, {
        headers: {
          "Content-Type": "multipart/form-data",
        },
      });
      message.success("Form submitted successfully!");
    } catch (error) {
      console.error("form error ", error);
      message.error("Error submitting form");
    }
  };

  useEffect(() => {
    let itemData = [];
    let form_data = {};
    formConfig.module.map((data, index) => {
      let subItem = [];

      data.fields.map((field, i) => {
        if (data.type !== "section") {
          field.type = data.type;
        }

        if (field.value && field.value.length > 0)
          form_data = {
            ...form_data,
            [field.name]:
              field.type === "date"
                ? dayjs(field.value, "YYYY-MM-DD")
                : field.value,
          };
        subItem.push(getFormItem(field, i));
      });

      // itemData.push({
      //   key: index,
      //   label: data.name,
      //   children: subItem,
      //   showArrow: false,
      // });
      itemData.push({
        key: index,
        label: data.name,
        children: subItem,
      });
    });

    form.setFieldsValue(form_data);
    setFormData(form_data);

    setItems(itemData);
  }, [form]);

  const uploadButton = (
    <button
      style={{
        border: 0,
        background: "none",
      }}
      type="button"
    >
      {loading ? <LoadingOutlined /> : <PlusOutlined />}
      <div
        style={{
          marginTop: 8,
        }}
      >
        Upload
      </div>
    </button>
  );

  if (!formConfig) {
    return <div>Loading form...</div>;
  }

  return (
    <Form
      name="basic"
      onFinish={handleSubmit}
      autoComplete="off"
      labelCol={{
        span: 6,
      }}
      wrapperCol={{
        span: 18,
      }}
      style={{
        maxWidth: 800,
      }}
      form={form}
    >
      <h1>{formConfig.title}</h1>
      {/* <Collapse defaultActiveKey={[0]} items={items} /> */}
      <Tabs
        defaultActiveKey="1"
        onChange={(value) => {
          setCurrent(value);
        }}
        tabPosition={"top"}
        items={items}
      />
      <Button type="primary" style={{ marginTop: "10px" }} htmlType="submit">
        Add Details
      </Button>
    </Form>
  );
};

export default DynamicForm;
