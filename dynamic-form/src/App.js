import React from "react";
import { BrowserRouter as Router, Routes, Route } from "react-router-dom";
import Home from "./pages/Home";
import AgencyForm from "./pages/AgencyForm";
import AgentForm from "./pages/AgentForm";
import Navbar from "./components/Navbar";
import SideMenu from "./components/SideMenu";
import styled from "styled-components";
import { Col, Row } from "antd";
import Allocation from "./pages/Allocation";

const Container = styled.div`
  margin-top: 10px;
  padding: 0px;
  border-right: 1px solid #ddd;
  height: 86vh;
`;

const MainContainer = styled.div`
  margin: 20px 10px;
  padding: 20px;
  border: 1px solid #ddd;
  border-radius: 8px;
`;

const App = () => {
  return (
    <Router>
      <Navbar />

      <Row>
        <Container>
          <SideMenu />
        </Container>
        <Col span={18}>
          <MainContainer>
            <Routes>
              <Route exact path="/" element={<Home />} />
              <Route path="/agency-onboarding" element={<AgencyForm />} />
              <Route path="/agent-onboarding" element={<AgentForm />} />
              <Route path="/allocation" element={<Allocation />} />
              {/* <Route path="/compliance" component={Compliance} /> */}
            </Routes>
          </MainContainer>
        </Col>
      </Row>
    </Router>
  );
};

export default App;
