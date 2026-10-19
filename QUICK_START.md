# Quick Start Guide - Vocally Batch Project

## Prerequisites
- Conda environment `RTA` is set up and activated
- All dependencies are installed from `requirements.txt`
- Database and Kafka services are running

## Starting the System with TMUX

### Option 1: Automated TMUX Setup
```bash
# Run this script to automatically create and configure tmux sessions
./start_vocally_batch.sh
```

### Option 2: Manual TMUX Setup
```bash
# 1. Create new tmux session
tmux new-session -d -s vocally_batch

# 2. Split the session horizontally
tmux split-window -h

# 3. Configure first pane (Query Processing)
tmux send-keys -t 0 "conda activate RTA" Enter
tmux send-keys -t 0 "cd /mnt/data2/vocally_batch" Enter
tmux send-keys -t 0 "python vocally_batch_query_processing.py" Enter

# 4. Configure second pane (BentoML Service)
tmux send-keys -t 1 "conda activate RTA" Enter
tmux send-keys -t 1 "cd /mnt/data2/vocally_batch" Enter
tmux send-keys -t 1 "bentoml serve service:VocallyChatService" Enter
```

## TMUX Session Management

### Attach to Session
```bash
# Attach to the running session
tmux attach-session -t vocally_batch

# Or use the shorter form
tmux a -t vocally_batch
```

### Navigate Between Panes
- **Ctrl+b + Arrow Keys**: Navigate between panes
- **Ctrl+b + %**: Split pane vertically
- **Ctrl+b + "**: Split pane horizontally
- **Ctrl+b + x**: Close current pane

### Detach from Session
- **Ctrl+b + d**: Detach from session (keeps it running)

### List Sessions
```bash
# List all tmux sessions
tmux list-sessions

# Or shorter form
tmux ls
```

### Kill Session
```bash
# Kill the session (stops all processes)
tmux kill-session -t vocally_batch
```

## Manual Session Management (Alternative to TMUX)

### Terminal 1: Document Processing
```bash
# Activate conda environment
conda activate RTP

# Navigate to project directory
cd /mnt/data2/vocally_batch

# Start document processing
python vocally_batch_processing_version5.py
```

### Terminal 2: BentoML Service
```bash
# Activate conda environment
conda activate RTA

# Navigate to project directory
cd /mnt/data2/vocally_batch

# Start BentoML service
bentoml serve service:VocallyChatService
```

## Verification Commands

### Check if Services are Running
```bash
# Check Python processes
ps aux | grep python | grep -v grep

# Check specific processes
ps aux | grep vocally_batch_query_processing
ps aux | grep bentoml

# Check listening ports
netstat -tlnp | grep :8000  # BentoML default port
```

### Check Logs
```bash
# Real-time log monitoring
tail -f logs/vocally_combined.log

# Check specific log files
tail -f logs/vocally_chat_pipeline.log
tail -f logs/vocally_chat_service.log
```

### Test API Endpoint
```bash
# Test if BentoML service is responding
curl -X POST http://localhost:8000/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "sl-llm",
    "messages": [{
      "query": "test",
      "noteRef": [1],
      "chat_id": "test",
      "uniqueId": ["test-uuid"]
    }],
    "max_tokens": 100
  }'
```

## Troubleshooting

### Common Issues

#### 1. Conda Environment Not Found
```bash
# List available environments
conda env list

# Create environment if it doesn't exist
conda create -n RTA python=3.8

# Activate environment
conda activate RTA
```

#### 2. Port Already in Use
```bash
# Check what's using the port
lsof -i :8000

# Kill the process if needed
kill -9 <PID>
```

#### 3. Permission Denied
```bash
# Fix directory permissions
chmod -R 755 /mnt/data2/vocally_batch/
chown -R $USER:$USER /mnt/data2/vocally_batch/
```

#### 4. Import Errors
```bash
# Reinstall dependencies
pip install -r requirements.txt --force-reinstall

# Or update specific packages
pip install --upgrade langchain chromadb
```

## Service Status Commands

### Quick Health Check
```bash
# Create a health check script
cat > health_check.sh << 'EOF'
#!/bin/bash
echo "=== Vocally Batch Health Check ==="
echo "1. Checking Python processes..."
ps aux | grep python | grep -v grep | grep vocally
echo ""
echo "2. Checking BentoML service..."
curl -s http://localhost:8000/health || echo "Service not responding"
echo ""
echo "3. Checking log files..."
ls -la logs/ | head -5
echo ""
echo "4. Checking disk space..."
df -h /mnt/data2/vocally_batch
EOF

chmod +x health_check.sh
./health_check.sh
```

## Stopping the System

### Graceful Shutdown
```bash
# In tmux session, use Ctrl+C in each pane
# Or kill the session
tmux kill-session -t vocally_batch
```

### Force Shutdown
```bash
# Kill all related processes
pkill -f vocally_batch_query_processing
pkill -f bentoml
```

## Next Steps

After starting the system:
1. **Upload Documents**: Place files in the `UPLOAD/` directory
2. **Process Documents**: Use the data ingestion pipeline
3. **Test Queries**: Send queries through the API or Kafka
4. **Monitor Logs**: Watch for any errors or issues
5. **Scale if Needed**: Adjust worker processes and configurations

For detailed information, refer to the main `README.md` documentation. 