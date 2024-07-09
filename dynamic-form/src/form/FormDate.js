import React from "react";
import { Form, DatePicker } from "antd";

const FormDate = ({ field, index, onChange }) => {
  return (
    <Form.Item
      label={field.label}
      name={field.name}
      rules={[
        {
          required: field.required ? field.required : false,
          message: `"Please select ${field.name}!"`,
        },
      ]}
      key={index}
    >
      <DatePicker
        format={"YYYY-MM-DD"}
        onChange={(date, string) => onChange(date, string, field)}
      />
    </Form.Item>
  );
};

export default FormDate;
