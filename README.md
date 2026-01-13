# ditteau_data_v1
Version 1 of Ditteau Data Prototype Phase 1
## Local Development Setup

### 1. Clone Repository
```bash
git clone <https://github.com/lvanpelt16/ditteau_data_v1>
cd ditteau_data_v1
```

### 2. Configure Environment Variables
```bash
# Copy template
cp .env.example .env

# Edit .env and fill in your Snowflake credentials
nano .env
```

### 3. Install direnv (Optional but Recommended)
```bash
brew install direnv
echo 'eval "$(direnv hook zsh)"' >> ~/.zshrc
source ~/.zshrc

# Allow direnv in project
direnv allow
```

### 4. Test Connection
```bash
dbt debug
```