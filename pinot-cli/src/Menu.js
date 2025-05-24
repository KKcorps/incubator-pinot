import React, {useState} from 'react';
import {Box, Text, useInput, useApp} from 'ink';
import * as actions from './actions.js';

const Menu = () => {
  const {exit} = useApp();
  const [index, setIndex] = useState(0);
  const options = [
    {label: 'Init config', action: actions.actionInit},
    {label: 'Health check', action: actions.healthCheck},
    {label: 'List tables', action: actions.listTables},
    {label: 'Describe table', action: actions.describeTable},
    {label: 'Create table', action: actions.createTable},
    {label: 'Delete table', action: actions.deleteTable},
    {label: 'List schemas', action: actions.listSchemas},
    {label: 'Add schema', action: actions.addSchema},
    {label: 'Run query', action: actions.runQuery},
    {label: 'List segments', action: actions.listSegments},
    {label: 'Reload table', action: actions.reloadTable},
    {label: 'Exit', action: () => exit()}
  ];

  useInput(async (input, key) => {
    if (key.downArrow) {
      setIndex((index + 1) % options.length);
    } else if (key.upArrow) {
      setIndex((index - 1 + options.length) % options.length);
    } else if (key.return) {
      await options[index].action();
    }
  });

  return (
    <Box flexDirection="column">
      {options.map((option, i) => (
        <Text key={option.label} color={index === i ? 'green' : undefined}>
          {index === i ? '❯ ' : '  '}{option.label}
        </Text>
      ))}
    </Box>
  );
};

export default Menu;
