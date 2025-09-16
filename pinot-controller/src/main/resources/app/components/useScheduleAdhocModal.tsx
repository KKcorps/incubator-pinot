/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import React, { useContext, useState } from 'react';
import { DialogContent, FormControlLabel, Switch, makeStyles } from '@material-ui/core';
import { UnControlled as CodeMirror } from 'react-codemirror2';
import Dialog from './CustomDialog';
import PinotMethodUtils from '../utils/PinotMethodUtils';
import { NotificationContext } from './Notification/NotificationContext';

const jsonoptions = {
  lineNumbers: true,
  mode: 'application/json',
  styleActiveLine: true,
  gutters: ['CodeMirror-lint-markers'],
  theme: 'default',
};

const useStyles = makeStyles(() => ({
  queryOutput: {
    border: '1px solid #BDCCD9',
    '& .CodeMirror': { height: 532 },
  },
}));

export default function useScheduleAdhocModal() {
  const classes = useStyles();
  const [open, setOpen] = useState(false);
  const [value, setValue] = useState(`{}`);
  const [isDryRun, setIsDryRun] = useState(false);
  const { dispatch: notify } = useContext(NotificationContext);

  const handleClose = () => {
    setIsDryRun(false);
    setOpen(false);
  };
  const handleOpen = () => {
    setIsDryRun(false);
    setOpen(true);
  };

  const handleSheduleAdhoc = async () => {
    let payload = {};
    try {
      if (value && value.trim()) {
        payload = JSON.parse(value);
      }
    } catch (error) {
      notify({
        type: 'error',
        message: 'Invalid JSON payload. Please fix the task definition and try again.',
        show: true
      });
      return;
    }

    if (payload === null || Array.isArray(payload) || typeof payload !== 'object') {
      notify({
        type: 'error',
        message: 'Task payload must be a JSON object.',
        show: true
      });
      return;
    }

    if (isDryRun) {
      payload.dryRun = true;
    } else if (payload.dryRun) {
      delete payload.dryRun;
    }

    try {
      await PinotMethodUtils.executeTaskAction(payload);
      notify({
        type: 'success',
        message: isDryRun ? 'Dry run executed successfully.' : 'Adhoc task scheduled successfully.',
        show: true
      });
      handleClose();
    } catch (error) {
      const message = error?.response?.data || error?.message || 'Failed to execute task.';
      notify({
        type: 'error',
        message,
        show: true
      });
    }
  };

  const dialog = (
    <Dialog
      open={open}
      handleClose={handleClose}
      handleSave={handleSheduleAdhoc}
      title={`Schedule Adhoc`}
      size="md"
      disableBackdropClick={true}
      disableEscapeKeyDown={true}
    >
      <DialogContent>
        <FormControlLabel
          control={
            <Switch
              color="primary"
              checked={isDryRun}
              onChange={(event) => setIsDryRun(event.target.checked)}
            />
          }
          label="Dry run mode"
        />
        <CodeMirror
          options={jsonoptions}
          value={value}
          className={classes.queryOutput}
          autoCursor={false}
          onChange={(editor, d, value) => {
            setValue(value);
          }}
        />
      </DialogContent>
    </Dialog>
  );

  return {
    handleOpen,
    handleClose,
    handleSheduleAdhoc,
    dialog
  };
}
