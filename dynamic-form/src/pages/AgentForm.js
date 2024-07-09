import React, { useEffect, useState } from "react";
import DynamicForm from "../DynamicForm";

const AgentForm = () => {
  const [formConfig, setFormConfig] = useState(null);
  const [timelineItem, setTimelineItem] = useState(null);

  const setTimelineItems = (module) => {
    if (module) {
      let mainMenus = module.map((item) => {
        return { children: item.name };
      });
      setTimelineItem(mainMenus);
    }
  };

  useEffect(() => {
    // Fetch the form configuration from the JSON file
    fetch("/formConfig.json")
      .then((response) => {
        if (!response.ok) {
          throw new Error("Network response was not ok " + response.statusText);
        }
        return response.json();
      })
      .then((data) => {
        setFormConfig(data);
        setTimelineItems(data.module);
      })
      .catch((error) => {
        console.error("There was a problem with the fetch operation:", error);
      });
  }, []);

  if (!formConfig) {
    return <div>Loading form...</div>;
  }

  return (
    <div>
      <DynamicForm data={formConfig} />
    </div>
  );
};

export default AgentForm;
