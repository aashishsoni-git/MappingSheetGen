"""
ETL Mapping Generator - Streamlit UI
User-friendly interface for generating AI-powered ETL mappings
"""
import streamlit as st
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv
import sys

load_dotenv()

# Page configuration
st.set_page_config(
    page_title="ETL Mapping Generator",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .stProgress > div > div > div > div {
        background-color: #1f77b4;
    }
    .success-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #d4edda;
        border: 1px solid #c3e6cb;
        color: #155724;
    }
    .info-box {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #d1ecf1;
        border: 1px solid #bee5eb;
        color: #0c5460;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'results' not in st.session_state:
    st.session_state.results = None
if 'processing' not in st.session_state:
    st.session_state.processing = False

# Header
st.markdown('<div class="main-header">🤖 AI-Powered ETL Mapping Generator</div>', unsafe_allow_html=True)
st.markdown("---")

# Sidebar - Configuration
with st.sidebar:
    st.header("⚙️ Configuration")
    
    # Mode selection
    mode = st.radio(
        "Loading Mode",
        ["🤖 AI Flexible (Recommended)", "📋 Legacy Fixed Schema"],
        help="AI Flexible mode can handle any file format and structure"
    )
    use_flexible = mode.startswith("🤖")
    
    st.markdown("---")
    
    # Connection status
    st.subheader("📡 Connection Status")
    
    openai_key = os.getenv('OPENAI_API_KEY')
    sf_account = os.getenv('SF_ACCOUNT')
    
    if openai_key:
        st.success("✅ OpenAI Connected")
    else:
        st.error("❌ OpenAI Not Connected")
    
    if sf_account:
        st.success("✅ Snowflake Connected")
    else:
        st.error("❌ Snowflake Not Connected")
    
    st.markdown("---")
    
    # Stats
    st.subheader("📊 System Info")
    
    # Count files
    xml_files = len([f for f in os.listdir('data') if f.endswith('.xml')]) if os.path.exists('data') else 0
    ref_files = len([f for f in os.listdir('reference_data') if f.endswith(('.csv', '.xlsx'))]) if os.path.exists('reference_data') else 0
    
    st.metric("XML Files", xml_files)
    st.metric("Reference Files", ref_files)
    
    st.markdown("---")
    
    # About
    with st.expander("ℹ️ About"):
        st.markdown("""
        **ETL Mapping Generator**
        
        This tool uses AI to automatically generate ETL mappings from XML source files to Snowflake Silver layer tables.
        
        **Features:**
        - Auto-detect product type
        - Parse complex nested XML
        - Fetch live schema from Snowflake
        - AI-powered document understanding
        - Generate mappings with confidence scores
        
        **Version:** 1.0.0  
        **Updated:** October 2025
        """)

# Main content area
col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("📁 Step 1: Select XML File")
    
    # File uploader or file selector
    upload_option = st.radio(
        "Choose input method:",
        ["📂 Select from existing files", "📤 Upload new file"],
        horizontal=True
    )
    
    xml_file = None
    
    if upload_option == "📂 Select from existing files":
        # List existing XML files
        if os.path.exists('data'):
            xml_files_list = [f for f in os.listdir('data') if f.endswith('.xml')]
            if xml_files_list:
                selected_file = st.selectbox(
                    "Select XML file:",
                    xml_files_list,
                    help="Choose an XML file from the data directory"
                )
                xml_file = os.path.join('data', selected_file)
                
                # Show file info
                if xml_file:
                    file_size = os.path.getsize(xml_file)
                    st.info(f"📄 File: `{selected_file}` | Size: {file_size:,} bytes")
            else:
                st.warning("No XML files found in data/ directory")
        else:
            st.warning("data/ directory not found")
    
    else:
        # File uploader
        uploaded_file = st.file_uploader(
            "Upload XML file",
            type=['xml'],
            help="Upload your source XML file"
        )
        
        if uploaded_file:
            # Save uploaded file
            os.makedirs('data/uploads', exist_ok=True)
            xml_file = f"data/uploads/{uploaded_file.name}"
            with open(xml_file, 'wb') as f:
                f.write(uploaded_file.getbuffer())
            st.success(f"✅ Uploaded: {uploaded_file.name}")

with col2:
    st.subheader("🎯 Step 2: Product Code")
    
    product_option = st.radio(
        "Product detection:",
        ["🤖 Auto-detect (AI)", "✍️ Manual entry"],
        help="AI can detect product type from XML structure"
    )
    
    product_code = None
    if product_option == "✍️ Manual entry":
        product_code = st.text_input(
            "Enter product code:",
            placeholder="e.g., PA001, HO003, CP001",
            help="Optional: Specify product code manually"
        )

st.markdown("---")

# Process button
col1, col2, col3 = st.columns([1, 2, 1])
with col2:
    if st.button("🚀 Generate Mappings", type="primary", use_container_width=True, disabled=xml_file is None):
        if xml_file and os.path.exists(xml_file):
            st.session_state.processing = True
            st.session_state.results = None
            
            # Progress tracking
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            try:
                from main import ETLMappingPipeline
                
                # Configuration
                config = {
                    'openai_key': os.getenv('OPENAI_API_KEY'),
                    'snowflake_config': {
                        'account': os.getenv('SF_ACCOUNT'),
                        'user': os.getenv('SF_USER'),
                        'password': os.getenv('SF_PASSWORD'),
                        'warehouse': os.getenv('SF_WAREHOUSE'),
                        'database': os.getenv('SF_DATABASE'),
                        'schema': 'ETL_MAPPER',
                        'role': os.getenv('SF_ROLE')
                    }
                }
                
                # Create pipeline
                progress_bar.progress(10)
                status_text.text("🔧 Initializing pipeline...")
                pipeline = ETLMappingPipeline(config['openai_key'], config['snowflake_config'])
                
                # Step 1: Product detection
                progress_bar.progress(20)
                status_text.text("🔍 Detecting product type...")
                
                # Step 2: Extract XML
                progress_bar.progress(30)
                status_text.text("📊 Extracting XML metadata...")
                
                # Step 3: Load to Snowflake
                progress_bar.progress(40)
                status_text.text("☁️ Loading to Snowflake stage...")
                
                # Step 4: Fetch schema
                progress_bar.progress(50)
                status_text.text("🗄️ Fetching Silver layer schema...")
                
                # Step 5: Load reference data
                progress_bar.progress(60)
                status_text.text("📚 Loading reference data with AI...")
                
                # Step 6: Generate mappings
                progress_bar.progress(70)
                status_text.text("🤖 Generating mappings with AI... (this may take 30-60 seconds)")
                
                # Run pipeline
                results = pipeline.run(
                    xml_file,
                    product_code=product_code if product_code else None,
                    use_flexible_loader=use_flexible
                )
                
                # Complete
                progress_bar.progress(100)
                status_text.text("✅ Complete!")
                
                st.session_state.results = results
                st.session_state.processing = False
                
                # Success message
                st.balloons()
                st.success(f"✅ Successfully generated {len(results.mappings)} mappings!")
                
            except Exception as e:
                st.session_state.processing = False
                st.error(f"❌ Error: {str(e)}")
                st.exception(e)

st.markdown("---")

# Display results
if st.session_state.results:
    results = st.session_state.results
    
    st.header("📊 Generated Mappings")
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Mappings", len(results.mappings))
    
    with col2:
        avg_confidence = sum(m.confidence_score for m in results.mappings) / len(results.mappings)
        st.metric("Avg Confidence", f"{avg_confidence:.1%}")
    
    with col3:
        high_conf = len([m for m in results.mappings if m.confidence_score >= 0.8])
        st.metric("High Confidence (≥80%)", high_conf)
    
    with col4:
        low_conf = len([m for m in results.mappings if m.confidence_score < 0.5])
        st.metric("Low Confidence (<50%)", low_conf)
    
    st.markdown("---")
    
    # Convert to DataFrame
    mappings_df = pd.DataFrame([{
        'Source Node': m.source_node,
        'Target Table': m.target_table,
        'Target Column': m.target_column,
        'Transformation': m.transformation_logic or '(none)',
        'Confidence': f"{m.confidence_score:.1%}",
        'Reasoning': m.reasoning
    } for m in results.mappings])
    
    # Filters
    col1, col2 = st.columns(2)
    
    with col1:
        table_filter = st.multiselect(
            "Filter by Target Table:",
            options=mappings_df['Target Table'].unique(),
            default=mappings_df['Target Table'].unique()
        )
    
    with col2:
        confidence_threshold = st.slider(
            "Minimum Confidence:",
            min_value=0.0,
            max_value=1.0,
            value=0.0,
            step=0.1,
            format="%.0f%%"
        )
    
    # Apply filters
    filtered_df = mappings_df[
        (mappings_df['Target Table'].isin(table_filter)) &
        (mappings_df['Confidence'].str.rstrip('%').astype(float) / 100 >= confidence_threshold)
    ]
    
    # Display table
    st.dataframe(
        filtered_df,
        use_container_width=True,
        height=400
    )
    
    st.markdown("---")
    
    # Download options
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # CSV download
        csv = mappings_df.to_csv(index=False)
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name=f"mappings_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            use_container_width=True
        )
    
    with col2:
        # Excel download
        import io
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            mappings_df.to_excel(writer, sheet_name='Mappings', index=False)
        
        st.download_button(
            label="📥 Download Excel",
            data=buffer.getvalue(),
            file_name=f"mappings_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
    
    with col3:
        # JSON download
        json_data = mappings_df.to_json(orient='records', indent=2)
        st.download_button(
            label="📥 Download JSON",
            data=json_data,
            file_name=f"mappings_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            mime="application/json",
            use_container_width=True
        )
    
    # Detailed view
    with st.expander("🔍 View Detailed Mapping"):
        selected_idx = st.selectbox(
            "Select mapping to view details:",
            range(len(filtered_df)),
            format_func=lambda i: f"{filtered_df.iloc[i]['Source Node']} → {filtered_df.iloc[i]['Target Table']}.{filtered_df.iloc[i]['Target Column']}"
        )
        
        if selected_idx is not None:
            mapping = filtered_df.iloc[selected_idx]
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**Source:**")
                st.code(mapping['Source Node'], language='xml')
                
                st.markdown("**Transformation:**")
                st.code(mapping['Transformation'], language='sql')
            
            with col2:
                st.markdown("**Target:**")
                st.code(f"{mapping['Target Table']}.{mapping['Target Column']}", language='sql')
                
                st.markdown("**Confidence:**")
                st.progress(float(mapping['Confidence'].rstrip('%')) / 100)
                
                st.markdown("**Reasoning:**")
                st.info(mapping['Reasoning'])

# Footer
st.markdown("---")
st.markdown(
    """
    <div style='text-align: center; color: gray; padding: 1rem;'>
        ETL Mapping Generator v1.0 | Powered by OpenAI GPT-4 & Snowflake | © 2025
    </div>
    """,
    unsafe_allow_html=True
)
