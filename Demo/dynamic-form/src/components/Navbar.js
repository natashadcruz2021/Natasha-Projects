import React, { useState } from "react";
import { Avatar, Space } from "antd";
import styled from "styled-components";

const TopBar = styled.div`
  margin: 0 -8px;
  padding: 0 10px 10px;
  border-bottom: 1px solid #ddd;
`;

const Navbar = () => {
  const [current, setCurrent] = useState("mail");

  const onClick = (e) => {
    console.log("click ", e);
    setCurrent(e.key);
  };

  return (
    <TopBar>
      <Space size={26}>
        <Avatar
          size={64}
          style={{ backgroundColor: "#fde3cf", color: "#f56a00" }}
        >
          YuCollect
        </Avatar>
      </Space>
    </TopBar>
  );
};
export default Navbar;
