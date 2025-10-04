"""
ETL Mapping Generator - Enhanced Streamlit UI with 3-Stage Workflow
"""
import streamlit as st
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Import our modules
from main import ETLMappingPipeline
from utils.database_helper import DatabaseHelper
from etl.executor import ETLExecutor

# Page configuration
st.set_page_config(
    page_title="ETL Mapping Generator - Enhanced",
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
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'results' not in st.session_state:
    st.session_state.results = None
if 'xml_id' not in st.session_state:
    st.session_state.xml_id = None

# Get configuration
@st.cache_data
def get_config():
    return {
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

config = get_config()

# Initialize helper classes
try:
    pipeline = ETLMappingPipeline(config['openai_key'], config['snowflake_config'])
    db_helper = DatabaseHelper(config['snowflake_config'])
    etl_executor = ETLExecutor(config['snowflake_config'])
except Exception as e:
    st.error(f"Failed to initialize: {e}")
    st.stop()

# Header
st.markdown('<div class="main-header">🤖 AI-Powered ETL Mapping Generator - Enhanced</div>', unsafe_allow_html=True)
st.markdown("---")

# Sidebar
with st.sidebar:
    st.header("⚙️ Configuration")
    
    mode = st.radio(
        "Loading Mode",
        ["🤖 AI Flexible (Recommended)", "📋 Legacy Fixed Schema"],
    )
    use_flexible = mode.startswith("🤖")
    
    st.markdown("---")
    st.subheader("📡 Connection Status")
    
    if config['openai_key']:
        st.success("✅ OpenAI Connected")
    else:
        st.error("❌ OpenAI Not Connected")
    
    if config['snowflake_config']['account']:
        st.success("✅ Snowflake Connected")
    else:
        st.error("❌ Snowflake Not Connected")

# Main tabs
tab1, tab2, tab3, tab4 = st.tabs([
    "📤 1. Upload & Generate", 
    "✅ 2. Review & Approve",
    "⚡ 3. Execute ETL",
    "📊 4. Monitor & Reports"
])

# TAB 1: Upload & Generate
with tab1:
    st.header("📤 Step 1: Upload XML & Generate Mappings")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        upload_option = st.radio(
            "Choose input method:",
            ["📂 Select from existing files", "📤 Upload new file"],
            horizontal=True
        )
        
        xml_file = None
        
        if upload_option == "📂 Select from existing files":
            if os.path.exists('data'):
                xml_files_list = [f for f in os.listdir('data') if f.endswith('.xml')]
                if xml_files_list:
                    selected_file = st.selectbox("Select XML file:", xml_files_list)
                    xml_file = os.path.join('data', selected_file)
                    st.info(f"📄 File: `{selected_file}` | Size: {os.path.getsize(xml_file):,} bytes")
                else:
                    st.warning("No XML files found")
        else:
            uploaded_file = st.file_uploader("Upload XML file", type=['xml'])
            if uploaded_file:
                os.makedirs('data/uploads', exist_ok=True)
                xml_file = f"data/uploads/{uploaded_file.name}"
                with open(xml_file, 'wb') as f:
                    f.write(uploaded_file.getbuffer())
                st.success(f"✅ Uploaded: {uploaded_file.name}")
    
    with col2:
        product_option = st.radio("Product detection:", ["🤖 Auto-detect (AI)", "✍️ Manual entry"])
        product_code = None
        if product_option == "✍️ Manual entry":
            product_code = st.text_input("Enter product code:", placeholder="PA001, HO003, CP001")
    
    st.markdown("---")
    
    if st.button("🚀 Generate Mappings & Save to DB", type="primary", disabled=xml_file is None):
        if xml_file and os.path.exists(xml_file):
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            try:
                progress_bar.progress(10)
                status_text.text("🔍 Detecting product...")
                
                results = pipeline.run(xml_file, product_code=product_code, use_flexible_loader=use_flexible)
                
                progress_bar.progress(50)
                status_text.text("💾 Saving to database...")
                
                with open(xml_file, 'r') as f:
                    xml_content = f.read()
                
                detected_product = product_code or 'PA001'
                xml_id = db_helper.save_xml_to_stage(xml_file, xml_content, detected_product, 'streamlit_user')
                
                progress_bar.progress(70)
                count = db_helper.save_mappings_to_db(xml_id, results)
                
                progress_bar.progress(100)
                status_text.text("✅ Complete!")
                
                st.session_state.results = results
                st.session_state.xml_id = xml_id
                
                st.balloons()
                st.success(f"✅ Generated {len(results.mappings)} mappings | XML ID: `{xml_id}`")
                
            except Exception as e:
                st.error(f"❌ Error: {str(e)}")

# TAB 2: Review & Approve
with tab2:
    st.header("✅ Step 2: Review & Approve Mappings")
    
    try:
        pending_mappings = db_helper.load_pending_mappings()
        
        if not pending_mappings.empty:
            xml_ids = pending_mappings['xml_id'].unique()
            selected_xml = st.selectbox("Select XML:", xml_ids)
            
            mappings_df = pending_mappings[pending_mappings['xml_id'] == selected_xml].copy()
            st.info(f"📊 Reviewing {len(mappings_df)} mappings")
            
            mappings_df['approve'] = True
            
            edited_df = st.data_editor(
                mappings_df[['mapping_id', 'approve', 'source_node', 'target_table', 'target_column', 
                            'transformation_logic', 'confidence_score', 'reasoning', 'user_notes']],
                column_config={
                    "approve": st.column_config.CheckboxColumn("✅ Approve?", default=True),
                    "confidence_score": st.column_config.ProgressColumn("Confidence", min_value=0, max_value=1)
                },
                hide_index=True,
                use_container_width=True,
                height=400
            )
            
            st.markdown("---")
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                if st.button("✅ Approve Selected", type="primary"):
                    approved_df = edited_df[edited_df['approve'] == True]
                    if len(approved_df) > 0:
                        count = db_helper.approve_mappings(selected_xml, approved_df, 'streamlit_user')
                        st.success(f"✅ Approved {count} mappings!")
                        st.rerun()
            
            with col2:
                if st.button("💾 Save Changes"):
                    count = db_helper.update_mappings(selected_xml, edited_df)
                    st.info(f"💾 Saved {count} mappings")
            
            with col3:
                if st.button("❌ Reject All"):
                    count = db_helper.reject_mappings(selected_xml)
                    st.warning(f"❌ Rejected {count} mappings")
                    st.rerun()
            
            with col4:
                csv = edited_df.to_csv(index=False)
                st.download_button("📥 Download", csv, f"mappings_{selected_xml}.csv", "text/csv")
        else:
            st.info("ℹ️ No pending mappings. Generate mappings in Step 1 first.")
    except Exception as e:
        st.error(f"Error loading mappings: {e}")

# TAB 3: Execute ETL
with tab3:
    st.header("⚡ Step 3: Execute ETL to Silver Layer")
    
    try:
        approved_mappings = db_helper.load_approved_mappings()
        
        if not approved_mappings.empty:
            xml_options = approved_mappings['xml_id'].unique()
            selected_xml = st.selectbox("Select XML to process:", xml_options)
            mappings = approved_mappings[approved_mappings['xml_id'] == selected_xml]
            
            st.info(f"📊 Ready to load {len(mappings)} mappings")
            
            col1, col2 = st.columns(2)
            with col1:
                execution_mode = st.radio("Mode:", ["Validate Only", "Execute & Load"])
            with col2:
                batch_size = st.number_input("Batch Size", value=1000, min_value=100)
            
            st.markdown("---")
            
            if st.button("⚡ Start ETL", type="primary"):
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                def update_progress(progress, status):
                    progress_bar.progress(progress)
                    status_text.text(status)
                
                results = etl_executor.execute_etl_pipeline(
                    xml_id=selected_xml,
                    mappings=mappings,
                    mode=execution_mode,
                    batch_size=batch_size,
                    progress_callback=update_progress
                )
                
                if results['status'] == 'Success':
                    st.success("✅ ETL Completed!")
                    col1, col2, col3, col4 = st.columns(4)
                    col1.metric("Processed", results['rows_processed'])
                    col2.metric("Inserted", results['rows_inserted'])
                    col3.metric("Failed", results['rows_failed'])
                    col4.metric("Duration", f"{results['duration_sec']:.2f}s")
                else:
                    st.error(f"❌ Failed: {results.get('error')}")
        else:
            st.info("ℹ️ No approved mappings. Approve in Step 2 first.")
    except Exception as e:
        st.error(f"Error: {e}")

# TAB 4: Monitor & Reports
with tab4:
    st.header("📊 Step 4: Monitoring & Reports")
    
    try:
        st.subheader("📈 Recent Executions")
        history = db_helper.load_execution_history(limit=20)
        
        if not history.empty:
            st.dataframe(history, use_container_width=True, height=300)
        else:
            st.info("No execution history yet")
        
        st.markdown("---")
        st.subheader("🔍 Reconciliation Results")
        recon = db_helper.load_reconciliation_results(limit=10)
        
        if not recon.empty:
            for idx, row in recon.iterrows():
                icon = "✅" if row['reconciliation_status'] == 'Pass' else "⚠️"
                with st.expander(f"{icon} {row['execution_id']}"):
                    col1, col2, col3, col4 = st.columns(4)
                    col1.metric("Source", row['source_count'])
                    col2.metric("Target", row['target_count'])
                    col3.metric("Matches", row['match_count'])
                    col4.metric("Mismatches", row['mismatch_count'])
        else:
            st.info("No reconciliation data yet")
    except Exception as e:
        st.error(f"Error: {e}")

st.markdown("---")
st.markdown("<div style='text-align: center; color: gray;'>ETL Mapping Generator v2.0 | © 2025</div>", unsafe_allow_html=True)
