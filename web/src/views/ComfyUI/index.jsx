import { useState, useEffect, useCallback, useRef } from 'react';
import { showError, showSuccess, showInfo } from 'utils/common';
import { API } from 'utils/api';
import { Icon } from '@iconify/react';
import {
  Box,
  Card,
  CardContent,
  Grid,
  Typography,
  Stack,
  Button,
  TextField,
  Select,
  MenuItem,
  FormControl,
  InputLabel,
  Chip,
  LinearProgress,
  Divider,
  Alert,
  CircularProgress,
  Stepper,
  Step,
  StepLabel,
  StepContent,
  Paper,
  IconButton,
  Tooltip,
  Collapse,
  Switch,
  FormControlLabel,
  Slider,
  List,
  ListItem,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  Badge
} from '@mui/material';
import SubCard from 'ui-component/cards/SubCard';

// Status color/icon mappings
const STATUS_CONFIG = {
  pending: { color: 'default', icon: 'solar:clock-circle-bold-duotone', label: 'Pending' },
  queued: { color: 'info', icon: 'solar:reorder-bold-duotone', label: 'Queued' },
  executing: { color: 'warning', icon: 'solar:play-bold-duotone', label: 'Executing' },
  completed: { color: 'success', icon: 'solar:check-circle-bold-duotone', label: 'Completed' },
  error: { color: 'error', icon: 'solar:close-circle-bold-duotone', label: 'Error' }
};

export default function ComfyUI() {
  // State
  const [model, setModel] = useState('');
  const [namespace, setNamespace] = useState('');
  const [workflows, setWorkflows] = useState([]);
  const [selectedWorkflow, setSelectedWorkflow] = useState(null);
  const [params, setParams] = useState({});
  const [loading, setLoading] = useState(false);
  const [submitting, setSubmitting] = useState(false);
  const [tasks, setTasks] = useState([]); // [{taskId, promptId, status, result, outputUrls, createdAt}]
  const [queueInfo, setQueueInfo] = useState(null);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const pollRef = useRef(null);

  // Load workflows
  const fetchWorkflows = useCallback(async () => {
    if (!model) return;
    setLoading(true);
    try {
      const res = await API.get('/v1/comfyui/workflows', {
        params: { model, namespace: namespace || undefined }
      });
      if (res.data?.workflows) {
        setWorkflows(res.data.workflows);
      } else if (Array.isArray(res.data)) {
        setWorkflows(res.data);
      }
    } catch (err) {
      // Error handled by interceptor
    }
    setLoading(false);
  }, [model, namespace]);

  // Load queue info
  const fetchQueue = useCallback(async () => {
    if (!model) return;
    try {
      const res = await API.get('/v1/comfyui/queue', {
        params: { model, namespace: namespace || undefined }
      });
      if (res.data) {
        setQueueInfo(res.data);
      }
    } catch {
      // silent
    }
  }, [model, namespace]);

  // Select workflow
  const selectWorkflow = useCallback(
    (wf) => {
      setSelectedWorkflow(wf);
      // Initialize param values with defaults
      const defaults = {};
      if (wf?.params) {
        wf.params.forEach((p) => {
          if (p.default !== undefined && p.default !== null) {
            defaults[p.name] = p.default;
          } else {
            defaults[p.name] = '';
          }
        });
      }
      setParams(defaults);
    },
    []
  );

  // Submit workflow
  const submitWorkflow = useCallback(async () => {
    if (!selectedWorkflow || !model) return;
    setSubmitting(true);
    try {
      const res = await API.post(
        `/v1/comfyui/workflows/${selectedWorkflow.id}/run`,
        { params },
        { params: { model, namespace: namespace || undefined } }
      );

      const data = res.data;
      if (data?.task_id) {
        const newTask = {
          taskId: data.task_id,
          promptId: data.prompt_id,
          status: data.status || 'pending',
          result: null,
          outputUrls: null,
          createdAt: new Date().toISOString(),
          steps: [{ status: 'pending', time: new Date().toISOString(), message: 'Task submitted' }]
        };
        setTasks((prev) => [newTask, ...prev]);
        showSuccess('Task submitted: ' + data.task_id);
      }
    } catch {
      // handled
    }
    setSubmitting(false);
  }, [selectedWorkflow, model, namespace, params]);

  // Poll task status
  const pollTasks = useCallback(async () => {
    if (!model) return;
    const pendingTasks = tasks.filter((t) => t.status !== 'completed' && t.status !== 'error');
    if (pendingTasks.length === 0) return;

    for (const task of pendingTasks) {
      try {
        const res = await API.get(`/v1/comfyui/tasks/${task.taskId}`, {
          params: { model, namespace: namespace || undefined }
        });
        const data = res.data;
        if (data) {
          setTasks((prev) =>
            prev.map((t) => {
              if (t.taskId !== task.taskId) return t;
              const newSteps = [...t.steps];
              if (data.status !== t.status) {
                newSteps.push({
                  status: data.status,
                  time: new Date().toISOString(),
                  message: `Status: ${data.status}`
                });
              }
              return {
                ...t,
                status: data.status || t.status,
                result: data.result || t.result,
                outputUrls: data.output_urls || t.outputUrls,
                steps: newSteps
              };
            })
          );

          if (data.status === 'completed') {
            showInfo(`Task ${task.taskId.substring(0, 12)}... completed`);
          } else if (data.status === 'error') {
            showError(`Task ${task.taskId.substring(0, 12)}... failed`);
          }
        }
      } catch {
        // silent
      }
    }
  }, [model, namespace, tasks]);

  // Interrupt
  const handleInterrupt = useCallback(async () => {
    if (!model) return;
    try {
      await API.post('/v1/comfyui/interrupt', null, {
        params: { model, namespace: namespace || undefined }
      });
      showSuccess('Interrupt sent');
    } catch {
      // handled
    }
  }, [model, namespace]);

  // Auto-refresh polling
  useEffect(() => {
    if (autoRefresh && tasks.some((t) => t.status !== 'completed' && t.status !== 'error')) {
      pollRef.current = setInterval(() => {
        pollTasks();
        fetchQueue();
      }, 3000);
    }
    return () => {
      if (pollRef.current) clearInterval(pollRef.current);
    };
  }, [autoRefresh, tasks, pollTasks, fetchQueue]);

  // Param update handler
  const handleParamChange = (name, value) => {
    setParams((prev) => ({ ...prev, [name]: value }));
  };

  // Render param input based on type
  const renderParamInput = (param) => {
    const value = params[param.name] ?? param.default ?? '';
    const hasMinMax = param.min !== undefined && param.max !== undefined;

    switch (param.type) {
      case 'int':
      case 'float':
        if (hasMinMax) {
          return (
            <Box key={param.name} sx={{ mb: 2 }}>
              <Typography variant="body2" gutterBottom>
                {param.name}
                {param.required && <span style={{ color: '#f44336' }}> *</span>}
                {param.description && (
                  <Typography variant="caption" color="text.secondary" sx={{ ml: 1 }}>
                    {param.description}
                  </Typography>
                )}
              </Typography>
              <Stack direction="row" spacing={2} alignItems="center">
                <Slider
                  value={Number(value) || param.min || 0}
                  min={param.min}
                  max={param.max}
                  step={param.type === 'float' ? 0.1 : 1}
                  onChange={(_, v) => handleParamChange(param.name, v)}
                  valueLabelDisplay="auto"
                  sx={{ flex: 1 }}
                />
                <TextField
                  size="small"
                  type="number"
                  value={value}
                  onChange={(e) => handleParamChange(param.name, param.type === 'float' ? parseFloat(e.target.value) : parseInt(e.target.value))}
                  sx={{ width: 80 }}
                  inputProps={{ min: param.min, max: param.max }}
                />
              </Stack>
            </Box>
          );
        }
        return (
          <TextField
            key={param.name}
            fullWidth
            size="small"
            label={param.name + (param.required ? ' *' : '')}
            helperText={param.description}
            type="number"
            value={value}
            onChange={(e) => handleParamChange(param.name, param.type === 'float' ? parseFloat(e.target.value) : parseInt(e.target.value))}
            sx={{ mb: 2 }}
          />
        );
      case 'bool':
        return (
          <FormControlLabel
            key={param.name}
            control={
              <Switch
                checked={Boolean(value)}
                onChange={(e) => handleParamChange(param.name, e.target.checked)}
              />
            }
            label={param.name + (param.required ? ' *' : '')}
            sx={{ mb: 2, display: 'block' }}
          />
        );
      case 'string':
      default:
        return (
          <TextField
            key={param.name}
            fullWidth
            size="small"
            label={param.name + (param.required ? ' *' : '')}
            helperText={param.description}
            multiline={param.name.toLowerCase().includes('prompt')}
            rows={param.name.toLowerCase().includes('prompt') ? 3 : 1}
            value={value}
            onChange={(e) => handleParamChange(param.name, e.target.value)}
            sx={{ mb: 2 }}
          />
        );
    }
  };

  // Extract output images from result
  const getOutputMedia = (task) => {
    const media = [];
    if (task.outputUrls) {
      task.outputUrls.forEach((url) => media.push({ url, type: guessMediaType(url) }));
    }
    return media;
  };

  const guessMediaType = (url) => {
    const ext = url.split('.').pop()?.toLowerCase();
    if (['mp4', 'webm', 'avi'].includes(ext)) return 'video';
    return 'image';
  };

  return (
    <>
      <Stack direction="row" alignItems="center" justifyContent="space-between" mb={3}>
        <Stack direction="column" spacing={0.5}>
          <Typography variant="h2">ComfyUI</Typography>
          <Typography variant="subtitle1" color="text.secondary">
            Workflow-based Image & Video Generation
          </Typography>
        </Stack>
      </Stack>

      {/* Connection config */}
      <Card sx={{ mb: 3 }}>
        <CardContent>
          <Stack direction="row" spacing={2} alignItems="center">
            <TextField
              size="small"
              label="Model"
              placeholder="comfyui-sdxl"
              value={model}
              onChange={(e) => setModel(e.target.value)}
              sx={{ minWidth: 200 }}
            />
            <TextField
              size="small"
              label="Namespace (optional)"
              placeholder="worker-1"
              value={namespace}
              onChange={(e) => setNamespace(e.target.value)}
              sx={{ minWidth: 200 }}
            />
            <Button
              variant="contained"
              onClick={fetchWorkflows}
              disabled={!model || loading}
              startIcon={<Icon icon="solar:refresh-bold-duotone" width={18} />}
            >
              {loading ? 'Loading...' : 'Load Workflows'}
            </Button>
            <Box sx={{ flex: 1 }} />
            {queueInfo && (
              <Chip
                icon={<Icon icon="solar:reorder-bold-duotone" width={16} />}
                label={`Queue: ${JSON.stringify(queueInfo?.queue_running?.length || 0)} running, ${JSON.stringify(queueInfo?.queue_pending?.length || 0)} pending`}
                variant="outlined"
                size="small"
              />
            )}
          </Stack>
        </CardContent>
      </Card>

      {loading && <LinearProgress sx={{ mb: 2 }} />}

      <Grid container spacing={3}>
        {/* Left: Workflow list */}
        <Grid item xs={12} md={3}>
          <SubCard title="Workflows">
            {workflows.length === 0 ? (
              <Typography color="text.secondary" variant="body2" sx={{ p: 2, textAlign: 'center' }}>
                {model ? 'No workflows found. Load first.' : 'Enter a model and click Load.'}
              </Typography>
            ) : (
              <List disablePadding>
                {workflows.map((wf) => (
                  <ListItem key={wf.id} disablePadding>
                    <ListItemButton
                      selected={selectedWorkflow?.id === wf.id}
                      onClick={() => selectWorkflow(wf)}
                    >
                      <ListItemIcon sx={{ minWidth: 36 }}>
                        <Icon icon="solar:code-square-bold-duotone" width={22} />
                      </ListItemIcon>
                      <ListItemText
                        primary={wf.name || wf.id}
                        secondary={wf.description}
                        primaryTypographyProps={{ variant: 'body2', fontWeight: selectedWorkflow?.id === wf.id ? 700 : 400 }}
                        secondaryTypographyProps={{ variant: 'caption', noWrap: true }}
                      />
                      {wf.params && (
                        <Badge badgeContent={wf.params.length} color="primary" sx={{ mr: 1 }} />
                      )}
                    </ListItemButton>
                  </ListItem>
                ))}
              </List>
            )}

            <Divider sx={{ my: 1 }} />

            {/* Queue actions */}
            <Stack direction="row" spacing={1} sx={{ p: 1.5 }}>
              <Button
                size="small"
                variant="outlined"
                color="info"
                startIcon={<Icon icon="solar:reorder-bold-duotone" width={16} />}
                onClick={fetchQueue}
                disabled={!model}
                fullWidth
              >
                Queue
              </Button>
              <Button
                size="small"
                variant="outlined"
                color="error"
                startIcon={<Icon icon="solar:stop-bold" width={16} />}
                onClick={handleInterrupt}
                disabled={!model}
                fullWidth
              >
                Interrupt
              </Button>
            </Stack>
          </SubCard>
        </Grid>

        {/* Right: Workflow params + Task execution */}
        <Grid item xs={12} md={9}>
          {selectedWorkflow ? (
            <Stack spacing={3}>
              {/* Parameter form */}
              <SubCard
                title={
                  <Stack direction="row" alignItems="center" spacing={1}>
                    <Icon icon="solar:settings-bold-duotone" width={22} />
                    <span>{selectedWorkflow.name || selectedWorkflow.id}</span>
                    <Chip label={selectedWorkflow.id} size="small" variant="outlined" />
                  </Stack>
                }
                secondary={
                  <Button
                    variant="contained"
                    onClick={submitWorkflow}
                    disabled={submitting || !model}
                    startIcon={
                      submitting ? (
                        <CircularProgress size={16} color="inherit" />
                      ) : (
                        <Icon icon="solar:play-bold" width={18} />
                      )
                    }
                  >
                    {submitting ? 'Submitting...' : 'Run Workflow'}
                  </Button>
                }
              >
                {selectedWorkflow.description && (
                  <Alert severity="info" sx={{ mb: 2 }}>
                    {selectedWorkflow.description}
                  </Alert>
                )}

                {selectedWorkflow.params?.length > 0 ? (
                  <Box>{selectedWorkflow.params.map(renderParamInput)}</Box>
                ) : (
                  <Typography color="text.secondary">No configurable parameters.</Typography>
                )}
              </SubCard>

              {/* Task execution list */}
              {tasks.length > 0 && (
                <SubCard
                  title={
                    <Stack direction="row" alignItems="center" spacing={1}>
                      <Icon icon="solar:checklist-minimalistic-bold-duotone" width={22} />
                      <span>Tasks ({tasks.length})</span>
                    </Stack>
                  }
                  secondary={
                    <FormControlLabel
                      control={
                        <Switch
                          size="small"
                          checked={autoRefresh}
                          onChange={(e) => setAutoRefresh(e.target.checked)}
                        />
                      }
                      label="Auto-refresh"
                    />
                  }
                >
                  <Stack spacing={2}>
                    {tasks.map((task) => (
                      <TaskCard key={task.taskId} task={task} />
                    ))}
                  </Stack>
                </SubCard>
              )}
            </Stack>
          ) : (
            <Card sx={{ p: 6, textAlign: 'center' }}>
              <Icon icon="solar:code-square-bold-duotone" width={64} style={{ opacity: 0.3 }} />
              <Typography variant="h5" color="text.secondary" sx={{ mt: 2 }}>
                Select a workflow to get started
              </Typography>
              <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
                Choose a workflow from the left panel, configure parameters, and run it.
              </Typography>
            </Card>
          )}
        </Grid>
      </Grid>
    </>
  );
}

// Task execution card with stepper timeline
function TaskCard({ task }) {
  const [expanded, setExpanded] = useState(true);
  const statusConf = STATUS_CONFIG[task.status] || STATUS_CONFIG.pending;
  const media = [];

  // Extract output media from outputUrls
  if (task.outputUrls?.length > 0) {
    task.outputUrls.forEach((url) => {
      const ext = url.split('.').pop()?.toLowerCase();
      media.push({ url, type: ['mp4', 'webm', 'avi'].includes(ext) ? 'video' : 'image' });
    });
  }

  return (
    <Paper variant="outlined" sx={{ overflow: 'hidden' }}>
      {/* Header */}
      <Stack
        direction="row"
        alignItems="center"
        spacing={1.5}
        sx={{
          p: 1.5,
          bgcolor: 'action.hover',
          cursor: 'pointer'
        }}
        onClick={() => setExpanded(!expanded)}
      >
        <Icon icon={statusConf.icon} width={22} />
        <Chip label={statusConf.label} color={statusConf.color} size="small" />
        <Typography variant="body2" fontFamily="monospace" sx={{ flex: 1 }}>
          {task.taskId.substring(0, 24)}...
        </Typography>
        <Typography variant="caption" color="text.secondary">
          {new Date(task.createdAt).toLocaleTimeString()}
        </Typography>
        <IconButton size="small">
          <Icon icon={expanded ? 'solar:alt-arrow-up-bold' : 'solar:alt-arrow-down-bold'} width={16} />
        </IconButton>
      </Stack>

      <Collapse in={expanded}>
        <Box sx={{ p: 2 }}>
          {/* Execution timeline */}
          <Stepper orientation="vertical" activeStep={task.steps.length - 1}>
            {task.steps.map((step, idx) => {
              const stepConf = STATUS_CONFIG[step.status] || STATUS_CONFIG.pending;
              return (
                <Step key={idx} completed={true}>
                  <StepLabel
                    StepIconComponent={() => (
                      <Icon icon={stepConf.icon} width={20} color={stepConf.color === 'default' ? undefined : `var(--mui-palette-${stepConf.color}-main, inherit)`} />
                    )}
                  >
                    <Stack direction="row" spacing={1} alignItems="center">
                      <Typography variant="body2">{step.message}</Typography>
                      <Typography variant="caption" color="text.secondary">
                        {new Date(step.time).toLocaleTimeString()}
                      </Typography>
                    </Stack>
                  </StepLabel>
                </Step>
              );
            })}
          </Stepper>

          {/* Executing indicator */}
          {(task.status === 'executing' || task.status === 'pending' || task.status === 'queued') && (
            <LinearProgress
              sx={{ mt: 2, borderRadius: 1 }}
              color={task.status === 'executing' ? 'warning' : 'info'}
            />
          )}

          {/* Output media */}
          {media.length > 0 && (
            <Box sx={{ mt: 2 }}>
              <Typography variant="subtitle2" gutterBottom>
                <Icon icon="solar:gallery-bold-duotone" width={18} style={{ verticalAlign: 'text-bottom', marginRight: 4 }} />
                Outputs ({media.length})
              </Typography>
              <Grid container spacing={1}>
                {media.map((m, idx) => (
                  <Grid item xs={12} sm={6} md={4} key={idx}>
                    {m.type === 'video' ? (
                      <video
                        controls
                        src={m.url}
                        style={{
                          width: '100%',
                          borderRadius: 8,
                          maxHeight: 300,
                          objectFit: 'contain',
                          background: '#000'
                        }}
                      />
                    ) : (
                      <a href={m.url} target="_blank" rel="noopener noreferrer">
                        <img
                          src={m.url}
                          alt={`output-${idx}`}
                          style={{
                            width: '100%',
                            borderRadius: 8,
                            maxHeight: 300,
                            objectFit: 'contain',
                            background: 'rgba(0,0,0,0.05)',
                            cursor: 'pointer'
                          }}
                        />
                      </a>
                    )}
                    <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5, display: 'block', wordBreak: 'break-all' }}>
                      {m.url.split('/').pop()}
                    </Typography>
                  </Grid>
                ))}
              </Grid>
            </Box>
          )}
        </Box>
      </Collapse>
    </Paper>
  );
}
