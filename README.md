
# 🤖 AI-Powered ETL Mapping Generator

An intelligent ETL mapping tool that uses OpenAI GPT-4 to automatically generate data mappings from XML source files to Snowflake Silver layer tables.

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![Streamlit](https://img.shields.io/badge/Streamlit-1.28+-red.svg)
![OpenAI](https://img.shields.io/badge/OpenAI-GPT--4-green.svg)
![Snowflake](https://img.shields.io/badge/Snowflake-Connected-00ADD8.svg)
![License](https://img.shields.io/badge/License-MIT-yellow.svg)

## 🌟 Features

### **Smart AI Core**
- 🤖 **Auto-detects product types** from XML structure (67-80% accuracy)
- 🧠 **Understands ANY document format** - Excel, CSV, JSON, TXT (no hardcoded schemas)
- 📚 **Learns from historical mappings** - Uses past examples to improve predictions
- 🎯 **Confidence scores** - Know which mappings to review

### **Flexible Architecture**
- 📂 **No hardcoded schemas** - AI infers file structure dynamically
- 🔄 **Handles complex XML** - Parses 200+ deeply nested nodes
- 🗄️ **Live schema fetching** - Pulls current schema from Snowflake
- 🌐 **Multi-product support** - Works with Auto, Homeowners, Commercial, etc.

### **Beautiful UI**
- 🎨 **Professional Streamlit interface** - Clean, intuitive design
- 📊 **Real-time progress tracking** - See each step as it happens
- 🔍 **Interactive filtering** - Filter by table, confidence threshold
- 💾 **Multiple export formats** - CSV, Excel, JSON downloads

### **Production Ready**
- ✅ **Comprehensive error handling** - Graceful failure recovery
- 📝 **Full logging system** - Track all operations
- 🧪 **Complete test suite** - 7 automated tests
- 🔐 **Secure credentials** - Environment-based configuration

## 🚀 Quick Start

### Prerequisites
- Python 3.9+
- OpenAI API key
- Snowflake account

### Installation

