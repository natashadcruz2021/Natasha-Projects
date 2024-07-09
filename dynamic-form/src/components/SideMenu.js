import React, { useState } from "react";
import {
  UserAddOutlined,
  DeploymentUnitOutlined,
  ProfileOutlined,
} from "@ant-design/icons";
import { Link } from "react-router-dom";
import { Menu } from "antd";
const items = [
  {
    key: "1",
    icon: <DeploymentUnitOutlined />,
    label: (
      <Link to="/agency-onboarding">
        <span className="nav-text">Agency Onboarding</span>
      </Link>
    ),
  },
  {
    key: "2",
    icon: <UserAddOutlined />,
    label: "Agent",
    children: [
      {
        key: "21",
        label: <Link to="/agent-onboarding">Agent Onboarding</Link>,
      },
      {
        key: "22",
        label: <Link to="/agent-list">Agent List</Link>,
      },
    ],
  },
  {
    key: "3",
    icon: <ProfileOutlined />,
    label: "Compliance",
  },
];
const Navbar = () => {
  const [current, setCurrent] = useState("mail");

  const onClick = (e) => {
    console.log("click ", e);
    setCurrent(e.key);
  };

  return (
    <Menu
      onClick={onClick}
      selectedKeys={[current]}
      mode="inline"
      items={items}
      style={{ width: "100%" }}
    />
  );
};
export default Navbar;
