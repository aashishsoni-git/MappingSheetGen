"""
ETL Mapping Generator - Enhanced - FIXED for empty results
"""
import streamlit as st
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

from main import ETLMappingPipeline
from utils.database_helper import DatabaseHelper
from etl.executor import ETLExecutor

st.set_page_config(
    page_title="ETL Mapping Generator - Enhanced",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

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

if 'results' not in st.session_state:
    st.session_state.results = None
if 'xml_id' not in st.session_state:
    st.session_state.xml_id = None

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

try:
    pipeline = ETLMappingPipeline(config['openai_key'], config['snowflake_config'])
    db_helper = DatabaseHelper(config['snowflake_config'])
    etl_executor = ETLExecutor(config['snowflake_config'])
except Exception as e:
    st.error(f"Failed to initialize: {e}")
    st.stop()

st.markdown('<div class="main-header">🤖 AI-Powered ETL Mapping Generator - Enhanced</div>', unsafe_allow_html=True)
st.markdown("---")

with st.sidebar:
    st.header("⚙️ Configuration")
    mode = st.radio("Loading Mode", ["🤖 AI Flexible (Recommended)", "📋 Legacy Fixed Schema"])
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
                status_text.text("🔍 Running pipeline...")
                
                results = pipeline.run(
                    xml_file,
                    product_code=product_code,
                    use_flexible_loader=use_flexible
                )
                
                progress_bar.progress(60)
                status_text.text("💾 Saving to database...")
                
                # ✅ Check if we got results
                if not results or not hasattr(results, 'mappings') or not results.mappings:
                    progress_bar.progress(100)
                    status_text.text("⚠️ No mappings generated")
                    st.warning("⚠️ No mappings were generated. The XML file may not have recognizable data nodes.")
                    st.info("💡 Try a different XML file or check if the file contains valid data.")
              # REPLACE WITH THIS CODE:
                else:
                    # ✅ Load XML to staging table
                    detected_product = product_code or 'PA001'
                    if results.mappings:
                        first_table = results.mappings[0].target_table
                        if '.' in first_table:
                            detected_product = first_table.split('.')[0]
                    
                    progress_bar.progress(70)
                    status_text.text("📦 Loading XML to staging table...")
                    
                    # ✅ This will load XML data into staging table
                    xml_id = db_helper.save_xml_raw_bronze(
                        xml_file, 
                        detected_product, 
                        'streamlit_user'
                    )

                    
                    progress_bar.progress(80)
                    status_text.text("💾 Saving mappings...")
                    
                    count = db_helper.save_mappings_to_db(xml_id, results)
                    
                    progress_bar.progress(100)
                    status_text.text("✅ Complete!")
                    
                    st.session_state.results = results
                    st.session_state.xml_id = xml_id
                    
                    st.balloons()
                    
                    st.success(f"""
                    ✅ **Success!**
                    - Generated {len(results.mappings)} mappings
                    - XML ID: `{xml_id}`
                    - Saved {count} mappings to database
                    """)
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Total Mappings", len(results.mappings))
                    with col2:
                        # ✅ Safe confidence calculation
                        if results.mappings:
                            avg_conf = sum(m.confidence_score for m in results.mappings) / len(results.mappings)
                            st.metric("Avg Confidence", f"{avg_conf:.1%}")
                        else:
                            st.metric("Avg Confidence", "N/A")
                    with col3:
                        st.metric("Saved to DB", count)
                    
                    # Display mappings preview
                    st.subheader("📊 Generated Mappings Preview")
                    preview_df = pd.DataFrame([{
                        'Source Node': m.source_node,
                        'Target Table': m.target_table,
                        'Target Column': m.target_column,
                        'Transformation': m.transformation_logic or '(none)',
                        'Confidence': f"{m.confidence_score:.1%}"
                    } for m in results.mappings[:50]])
                    
                    st.dataframe(preview_df, use_container_width=True, height=400)
                    
                    if len(results.mappings) > 50:
                        st.info(f"📋 Showing 50 of {len(results.mappings)} mappings.")
                
            except Exception as e:
                progress_bar.progress(100)
                status_text.text("❌ Error!")
                st.error(f"❌ Error: {str(e)}")
                with st.expander("🔍 Debug Details"):
                    import traceback
                    st.code(traceback.format_exc())

# TAB 2: Review & Approve
with tab2:
    st.header("✅ Step 2: Review & Approve Mappings")
    
    try:
        pending_mappings = db_helper.load_pending_mappings()
        
        if not pending_mappings.empty:
            if 'xml_id' not in pending_mappings.columns:
                st.error("❌ Database schema issue: 'xml_id' column missing.")
            else:
                xml_ids = pending_mappings['xml_id'].unique()
                
                selected_xml = st.selectbox(
                    "Select XML to review:",
                    xml_ids,
                    format_func=lambda x: f"{x} ({len(pending_mappings[pending_mappings['xml_id']==x])} mappings)"
                )
                
                mappings_df = pending_mappings[pending_mappings['xml_id'] == selected_xml].copy()
                st.info(f"📊 Reviewing {len(mappings_df)} mappings for XML: `{selected_xml}`")
                
                mappings_df['approve'] = True
                
                if 'user_notes' not in mappings_df.columns:
                    mappings_df['user_notes'] = ''
                
                edited_df = st.data_editor(
                    mappings_df[['mapping_id', 'approve', 'source_node', 'target_table', 'target_column', 
                                'transformation_logic', 'confidence_score', 'reasoning', 'user_notes']],
                    column_config={
                        "approve": st.column_config.CheckboxColumn("✅ Approve?", default=True),
                        "confidence_score": st.column_config.ProgressColumn("Confidence", min_value=0, max_value=1),
                        "transformation_logic": st.column_config.TextColumn("Transformation", width="medium"),
                        "user_notes": st.column_config.TextColumn("Notes", width="medium")
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
                        else:
                            st.warning("No mappings selected")
                
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



# Initialize session state for debug logs
if 'debug_logs' not in st.session_state:
    st.session_state.debug_logs = []
if 'last_execution_summary' not in st.session_state:
    st.session_state.last_execution_summary = None

# TAB 3: Execute ETL
# with tab3:
    st.header("⚡ Step 3: Execute ETL to Silver Layer")
    
    # ✅ Persistent Debug Panel
    with st.expander("🐛 Debug Console (Persistent)", expanded=True):
        if st.session_state.debug_logs:
            for log in st.session_state.debug_logs[-20:]:  # Show last 20 logs
                st.text(log)
        else:
            st.info("No debug logs yet. Click Execute ETL to see logs.")
        
        if st.button("🗑️ Clear Debug Logs"):
            st.session_state.debug_logs = []
            st.rerun()
    
    # Show last execution summary if available
    if st.session_state.last_execution_summary:
        st.info(f"""
        **Last Execution:**
        - Execution ID: {st.session_state.last_execution_summary['execution_id']}
        - Tables Processed: {st.session_state.last_execution_summary['tables_processed']}
        - Rows Inserted: {st.session_state.last_execution_summary['total_rows']}
        """)
    
    try:
        approved_mappings = db_helper.load_approved_mappings()
        
        if approved_mappings.empty:
            st.info("ℹ️ No approved mappings. Approve mappings in Step 2 first.")
        elif 'xml_id' not in approved_mappings.columns:
            st.error("❌ Database schema issue: 'xml_id' column missing.")
            with st.expander("🔍 Debug: Available Columns"):
                st.write("Columns returned:", list(approved_mappings.columns))
                st.dataframe(approved_mappings.head())
        else:
            xml_ids = approved_mappings['xml_id'].unique()
            
            st.info(f"📊 Found {len(approved_mappings)} approved mappings for {len(xml_ids)} XML file(s)")
            
            selected_xml = st.selectbox(
                "Select XML to execute:",
                xml_ids,
                format_func=lambda x: f"{x} ({len(approved_mappings[approved_mappings['xml_id']==x])} mappings)"
            )
            
            xml_mappings = approved_mappings[approved_mappings['xml_id'] == selected_xml]
            
            # Debug: Show mapping structure
            with st.expander("🔍 Debug: Mapping Structure", expanded=False):
                st.write(f"**DataFrame Shape:** {xml_mappings.shape}")
                st.write(f"**Columns:** {list(xml_mappings.columns)}")
                st.write(f"**Data Types:**")
                st.write(xml_mappings.dtypes)
                st.write(f"**Sample Rows:**")
                st.dataframe(xml_mappings.head(3))
            
            # Display preview
            st.subheader(f"📋 Mappings for {selected_xml}")
            display_cols = ['source_node', 'target_table', 'target_column', 'transformation_logic', 'confidence_score']
            available_cols = [col for col in display_cols if col in xml_mappings.columns]
            st.dataframe(xml_mappings[available_cols], use_container_width=True, height=300)
            
            # Show summary
            tables = xml_mappings['target_table'].unique()
            st.markdown(f"**Target Tables ({len(tables)}):** {', '.join(tables)}")
            
            # Check Staging Data Button
            if st.button("🔍 Check Staging Data", key="check_staging"):
                st.session_state.debug_logs.append(f"[{datetime.now()}] Checking staging data for {selected_xml}")
                
                conn = etl_executor.get_connection()
                cursor = conn.cursor()
                
                st.subheader("Staging Data Analysis")
                
                # Check XML_STAGING table
                try:
                    cursor.execute(f"""
                        SELECT xml_id, 
                               COUNT(*) as total_rows,
                               SUM(CASE WHEN processed = TRUE THEN 1 ELSE 0 END) as processed_rows,
                               SUM(CASE WHEN processed = FALSE THEN 1 ELSE 0 END) as pending_rows
                        FROM INSURANCE.ETL_MAPPER.XML_STAGING
                        WHERE xml_id = '{selected_xml}'
                        GROUP BY xml_id
                    """)
                    result = cursor.fetchone()
                    if result:
                        msg = f"XML_STAGING: {result[1]} rows ({result[3]} pending)"
                        st.success(msg)
                        st.session_state.debug_logs.append(f"[{datetime.now()}] {msg}")
                        
                        # Show sample data structure
                        cursor.execute(f"""
                            SELECT xml_data
                            FROM INSURANCE.ETL_MAPPER.XML_STAGING
                            WHERE xml_id = '{selected_xml}'
                            LIMIT 1
                        """)
                        sample = cursor.fetchone()
                        if sample:
                            st.write("**Sample XML Data Structure:**")
                            st.json(sample[0])
                            st.session_state.debug_logs.append(f"[{datetime.now()}] Sample data retrieved")
                    else:
                        msg = f"No data in XML_STAGING for {selected_xml}"
                        st.warning(msg)
                        st.session_state.debug_logs.append(f"[{datetime.now()}] {msg}")
                except Exception as e:
                    msg = f"XML_STAGING check failed: {str(e)}"
                    st.warning(msg)
                    st.session_state.debug_logs.append(f"[{datetime.now()}] ERROR: {msg}")
                
                cursor.close()
                conn.close()
            
            st.markdown("---")
            col1, col2, col3 = st.columns([1, 1, 2])
            
            with col1:
                if st.button("🚀 Execute ETL", type="primary"):
                    # Clear previous logs for this execution
                    st.session_state.debug_logs.append(f"\n{'='*60}")
                    st.session_state.debug_logs.append(f"[{datetime.now()}] NEW EXECUTION STARTED")
                    st.session_state.debug_logs.append(f"[{datetime.now()}] XML ID: {selected_xml}")
                    st.session_state.debug_logs.append(f"[{datetime.now()}] Mappings count: {len(xml_mappings)}")
                    st.session_state.debug_logs.append(f"[{datetime.now()}] Unique tables: {len(tables)}")
                    st.session_state.debug_logs.append(f"[{datetime.now()}] Tables list: {list(tables)}")
                    
                    with st.spinner("⏳ Executing ETL & Generating Views..."):
                        try:
                            # Add detailed logging before execution
                            st.write("**Pre-execution Debug:**")
                            st.write(f"- xml_id: `{selected_xml}`")
                            st.write(f"- mappings count: {len(xml_mappings)}")
                            st.write(f"- unique tables: {len(tables)}")
                            
                            # Log DataFrame info
                            st.session_state.debug_logs.append(f"[{datetime.now()}] DataFrame columns: {list(xml_mappings.columns)}")
                            st.session_state.debug_logs.append(f"[{datetime.now()}] DataFrame shape: {xml_mappings.shape}")
                            
                            # Execute
                            st.session_state.debug_logs.append(f"[{datetime.now()}] Calling execute_mappings()...")
                            summary = etl_executor.execute_mappings(selected_xml, xml_mappings)
                            
                            st.session_state.debug_logs.append(f"[{datetime.now()}] Execution completed")
                            st.session_state.debug_logs.append(f"[{datetime.now()}] Summary: {summary}")
                            
                            # Store in session state
                            st.session_state.last_execution_summary = summary
                            
                            st.success(f"""
                            ✅ **ETL Execution Complete!**
                            - Execution ID: `{summary['execution_id']}`
                            - Tables Processed: {summary['tables_processed']}
                            - Total Rows Inserted: {summary['total_rows']}
                            - Views Generated: {len(summary['view_queries'])}
                            """)
                            
                            if summary['successful_tables']:
                                st.write("**✅ Successful Tables:**")
                                for table in summary['successful_tables']:
                                    st.write(f"  - {table}")
                            
                            if summary['failed_tables']:
                                st.error("**❌ Failed Tables:**")
                                for i, table in enumerate(summary['failed_tables']):
                                    st.write(f"  - {table}")
                                    if i < len(summary['errors']):
                                        st.code(summary['errors'][i])
                                        st.session_state.debug_logs.append(f"[{datetime.now()}] ERROR in {table}: {summary['errors'][i]}")
                            
                            # Display VIEW queries
                            if summary['view_queries']:
                                st.markdown("---")
                                st.subheader("📄 Generated VIEW Queries")
                                
                                for table, view_query in summary['view_queries'].items():
                                    with st.expander(f"📊 View: {table.split('.')[-1]}_VW"):
                                        st.code(view_query, language='sql')
                                        st.download_button(
                                            f"📥 Download SQL",
                                            view_query,
                                            f"{table.split('.')[-1]}_VW.sql",
                                            "text/plain",
                                            key=f"download_{table}"
                                        )
                            
                            # DON'T rerun so debug info persists
                            # st.rerun()  # REMOVED
                            
                        except Exception as e:
                            error_msg = f"Execution failed: {str(e)}"
                            st.session_state.debug_logs.append(f"[{datetime.now()}] CRITICAL ERROR: {error_msg}")
                            
                            st.error(f"❌ {error_msg}")
                            with st.expander("🔍 Full Error Traceback"):
                                import traceback
                                tb = traceback.format_exc()
                                st.code(tb)
                                st.session_state.debug_logs.append(f"[{datetime.now()}] Traceback: {tb}")
            
            with col2:
                if st.button("📚 View Saved Definitions"):
                    try:
                        saved_views = etl_executor.get_saved_views(selected_xml)
                        if not saved_views.empty:
                            st.dataframe(saved_views[['view_name', 'target_table', 'created_at']], 
                                       use_container_width=True)
                            
                            for _, row in saved_views.iterrows():
                                with st.expander(f"📄 {row['view_name']}"):
                                    st.code(row['view_query'], language='sql')
                        else:
                            st.info("No saved views yet for this XML")
                    except Exception as e:
                        st.error(f"Error: {e}")
            
            with col3:
                st.info("💡 This will:\n- Insert data from XML to Silver tables\n- Generate reusable VIEW queries\n- Save VIEWs for future loads")
            
    except Exception as e:
        error_msg = f"Tab Error: {str(e)}"
        st.session_state.debug_logs.append(f"[{datetime.now()}] TAB ERROR: {error_msg}")
        st.error(error_msg)
        with st.expander("🔍 Debug"):
            import traceback
            st.code(traceback.format_exc())

# In TAB 3, find this section:
# In TAB 3, find this section:
# ==================== TAB 3: Execute ETL ====================
# ==================== TAB 3: Execute ETL ====================
with tab3:
    st.header("⚙️ Step 3: Generate SQL VIEWs")
    
    # ========== Debug Console (Persistent) ==========
    st.subheader("🐛 Debug Console (Persistent)")
    
    if 'debug_logs' not in st.session_state:
        st.session_state.debug_logs = []
    
    debug_container = st.container()
    with debug_container:
        if st.session_state.debug_logs:
            for log in st.session_state.debug_logs[-20:]:  # Show last 20 logs
                st.text(log)
        else:
            st.info("No debug logs yet. Click 'Generate SQL VIEWs' to see logs.")
    
    # Show last execution summary
    if 'last_execution' in st.session_state:
        st.markdown("### Last Execution:")
        exec_summary = st.session_state.last_execution
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Execution ID", exec_summary.get('execution_id', 'N/A'))
        with col2:
            st.metric("VIEWs Created", len(exec_summary.get('views_created', [])))
        with col3:
            st.metric("Errors", len(exec_summary.get('errors', [])))
    
    st.markdown("---")
    
    # ========== Load Approved Mappings ==========
    try:
        approved_mappings = db_helper.load_approved_mappings()
        
        if approved_mappings.empty:
            st.warning("⚠️ No approved mappings found. Please approve mappings in Step 2 first.")
            st.stop()
        
        # Group by xml_id
        xml_groups = approved_mappings.groupby('xml_id').agg({
            'mapping_id': 'count',
            'target_table': lambda x: x.nunique()
        }).reset_index()
        xml_groups.columns = ['xml_id', 'mapping_count', 'table_count']
        
        st.success(f"📊 Found {len(xml_groups)} approved mapping set(s) for {len(xml_groups)} XML file(s)")
        
    except Exception as e:
        st.error(f"Failed to load approved mappings: {e}")
        st.stop()
    
    # ========== XML Selector ==========
    st.subheader("Select XML to process:")
    
    xml_options = [
        f"{row['xml_id']} ({row['mapping_count']} mappings, {row['table_count']} tables)"
        for _, row in xml_groups.iterrows()
    ]
    
    selected_option = st.selectbox("Choose XML file:", options=xml_options, key="xml_selector_tab3")
    selected_xml = selected_option.split(' ')[0]  # Extract xml_id
    
    # Get mappings for selected XML
    xml_mappings = approved_mappings[approved_mappings['xml_id'] == selected_xml].copy()
    
    # ========== Debug: Mapping Structure ==========
    with st.expander("🔍 Debug: Mapping Structure", expanded=False):
        st.write(f"**Total mappings:** {len(xml_mappings)}")
        st.write(f"**Columns in DataFrame:** {list(xml_mappings.columns)}")
        st.dataframe(xml_mappings.head())
    
    # ========== Show Mapping Summary ==========
    st.markdown(f"### 📋 Mappings for {selected_xml}")
    
    table_groups = xml_mappings.groupby('target_table').size()
    col1, col2 = st.columns([1, 3])
    
    with col1:
        st.write(f"**Target Tables ({len(table_groups)}):**")
        for table in table_groups.index:
            st.write(f"- {table}")
    
    with col2:
        st.write(f"**Mapping counts by table:**")
        st.dataframe(table_groups, use_container_width=True)
    
    # ========== XML & Mapping Debug Button ==========
    st.markdown("---")
    st.subheader("🔍 Debug XML Extraction")
    
    if st.button("🔍 Show XML & Mapping Debug Info", key="debug_xml_btn"):
        with st.expander("📋 Debug Information", expanded=True):
            conn = etl_executor.get_connection()
            cursor = conn.cursor()
            
            try:
                # 1. Show XML data
                st.markdown("#### 1️⃣ Raw XML from Bronze Table")
                cursor.execute(f"""
                    SELECT raw_xml, LENGTH(raw_xml) as xml_length
                    FROM INSURANCE.ETL_MAPPER.XML_RAW_BRONZE
                    WHERE xml_id = '{selected_xml}'
                """)
                result = cursor.fetchone()
                
                if result:
                    xml_content = result[0]
                    xml_length = result[1]
                    
                    st.info(f"✅ XML Found - Size: {xml_length:,} bytes")
                    st.code(xml_content[:2000], language='xml')  # Show first 2000 chars
                    
                    if xml_length > 2000:
                        st.caption(f"... (showing first 2,000 of {xml_length:,} bytes)")
                    
                    # 2. Show what mappings expect for each table
                    st.markdown("#### 2️⃣ Mappings by Table")
                    
                    for table in xml_mappings['target_table'].unique():
                        with st.expander(f"📊 {table.split('.')[-1]}"):
                            table_maps = xml_mappings[xml_mappings['target_table'] == table]
                            st.dataframe(table_maps[['source_node', 'target_column', 'confidence_score']])
                    
                    # 3. Test extractions for POLICY table specifically
                    st.markdown("#### 3️⃣ Test Extraction Results (POLICY Table)")
                    policy_maps = xml_mappings[xml_mappings['target_table'] == 'SILVER.POLICY']
                    
                    if not policy_maps.empty:
                        for _, mapping in policy_maps.iterrows():
                            node = mapping['source_node']
                            col = mapping['target_column']
                            
                            # Skip ID columns
                            if '_ID' in col.upper():
                                continue
                            
                            # Build the XMLGET query
                            node_parts = node.split('/')
                            xml_expr = "xml_variant"
                            for part in node_parts:
                                xml_expr = f"XMLGET({xml_expr}, '{part}')"
                            
                            test_sql = f"""
                                SELECT {xml_expr}:"$"::STRING as value
                                FROM INSURANCE.ETL_MAPPER.XML_RAW_BRONZE
                                WHERE xml_id = '{selected_xml}'
                            """
                            
                            try:
                                cursor.execute(test_sql)
                                value = cursor.fetchone()[0]
                                
                                if value and value.strip():
                                    st.success(f"✅ **{col}** = `{value}`")
                                    st.caption(f"   Path: `{node}`")
                                else:
                                    st.warning(f"⚠️ **{col}** = NULL or empty")
                                    st.caption(f"   Path: `{node}` ← Check if this path is correct in XML")
                            except Exception as e:
                                st.error(f"❌ **{col}** extraction ERROR")
                                st.caption(f"   Path: `{node}`")
                                st.caption(f"   Error: {str(e)[:150]}")
                    else:
                        st.info("No POLICY table mappings found")
                    
                    # 4. Summary for all tables
                    st.markdown("#### 4️⃣ Extraction Summary (All Tables)")
                    
                    summary_data = []
                    for table in xml_mappings['target_table'].unique():
                        table_maps = xml_mappings[xml_mappings['target_table'] == table]
                        
                        success_count = 0
                        null_count = 0
                        error_count = 0
                        
                        for _, mapping in table_maps.iterrows():
                            if '_ID' in mapping['target_column'].upper():
                                continue
                            
                            node = mapping['source_node']
                            node_parts = node.split('/')
                            xml_expr = "xml_variant"
                            for part in node_parts:
                                xml_expr = f"XMLGET({xml_expr}, '{part}')"
                            
                            try:
                                cursor.execute(f"""
                                    SELECT {xml_expr}:"$"::STRING
                                    FROM INSURANCE.ETL_MAPPER.XML_RAW_BRONZE
                                    WHERE xml_id = '{selected_xml}'
                                """)
                                value = cursor.fetchone()[0]
                                if value and value.strip():
                                    success_count += 1
                                else:
                                    null_count += 1
                            except:
                                error_count += 1
                        
                        summary_data.append({
                            'Table': table.split('.')[-1],
                            '✅ Extracted': success_count,
                            '⚠️ NULL': null_count,
                            '❌ Errors': error_count
                        })
                    
                    import pandas as pd
                    st.dataframe(pd.DataFrame(summary_data), use_container_width=True)
                    
                else:
                    st.error(f"❌ No XML found in Bronze table for xml_id: {selected_xml}")
                    st.info("💡 Go to Step 1 and upload the XML file again to populate Bronze table")
                    
            except Exception as e:
                st.error(f"Debug failed: {e}")
                import traceback
                st.code(traceback.format_exc())
            finally:
                cursor.close()
                conn.close()
    
    # ========== Pre-execution Debug ==========
    st.markdown("---")
    with st.expander("🔍 Pre-execution Debug", expanded=False):
        st.write(f"**xml_id:** {selected_xml}")
        st.write(f"**mappings count:** {len(xml_mappings)}")
        st.write(f"**unique tables:** {xml_mappings['target_table'].nunique()}")
        st.write(f"**tables:** {list(xml_mappings['target_table'].unique())}")
    
    # ========== Generate VIEWs Button ==========
    st.markdown("---")
    
    col1, col2 = st.columns([2, 1])
    with col1:
        execute_button = st.button("🚀 Generate SQL VIEWs", type="primary", key="execute_etl_btn")
    with col2:
        st.info("💡 This will:\n- Generate SQL VIEWs from mappings\n- Create VIEWs in Snowflake\n- Test extraction (no data loading yet)")
    
        if execute_button:
        # Clear previous logs
            st.session_state.debug_logs = []
            st.session_state.debug_logs.append(f"{'='*60}")
            st.session_state.debug_logs.append(f"[{datetime.now()}] VIEW GENERATION STARTED")
            st.session_state.debug_logs.append(f"[{datetime.now()}] XML ID: {selected_xml}")
            st.session_state.debug_logs.append(f"[{datetime.now()}] Mappings: {len(xml_mappings)}")
            st.session_state.debug_logs.append(f"[{datetime.now()}] Tables: {list(xml_mappings['target_table'].unique())}")
            
            progress = st.progress(0)
            status_text = st.empty()
            
            try:
                status_text.text("⏳ Connecting to Snowflake...")
                progress.progress(20)
                
                status_text.text("⏳ Generating SQL VIEWs...")
                progress.progress(40)
                
                # Execute
                result = etl_executor.execute_mappings(selected_xml, xml_mappings)
                
                progress.progress(80)
                st.session_state.debug_logs.append(f"[{datetime.now()}] Generation completed")
                st.session_state.debug_logs.append(f"[{datetime.now()}] VIEWs created: {len(result.get('views_created', []))}")
                st.session_state.debug_logs.append(f"[{datetime.now()}] Errors: {len(result.get('errors', []))}")
                
                # Store in session state for persistence
                st.session_state.last_execution = result
                
                progress.progress(100)
                status_text.empty()
                progress.empty()
                
                # Display results
                if result.get('views_created'):
                    st.success(f"✅ SQL VIEWs Generated Successfully! ({len(result['views_created'])} VIEWs)")
                elif result.get('errors'):
                    st.error(f"❌ VIEW Generation Failed! ({len(result['errors'])} errors)")
                else:
                    st.warning("⚠️ No VIEWs created and no errors reported")
                
                # Show metrics
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Execution ID", result['execution_id'])
                with col2:
                    st.metric("VIEWs Created", len(result.get('views_created', [])))
                with col3:
                    st.metric("Errors", len(result.get('errors', [])))
                
                # Show created VIEWs
                if result.get('views_created'):
                    st.markdown("### ✅ Created VIEWs:")
                    for view in result['views_created']:
                        st.code(f"SELECT * FROM {view} LIMIT 10;", language='sql')
                    
                    # Show the SQL for each VIEW
                    st.markdown("### 📜 VIEW SQL Definitions:")
                    for table, sql in result.get('view_sqls', {}).items():
                        with st.expander(f"📊 {table.split('.')[-1]} VIEW SQL", expanded=False):
                            st.code(sql, language='sql')
                            
                            # Test button
                            view_name = [v for v in result['views_created'] if table.split('.')[-1] in v]
                            if view_name:
                                if st.button(f"🧪 Test {table.split('.')[-1]}", key=f"test_{table}"):
                                    with st.spinner(f"Testing {table.split('.')[-1]}..."):
                                        try:
                                            test_conn = etl_executor.get_connection()
                                            test_cursor = test_conn.cursor()
                                            test_cursor.execute(f"SELECT * FROM {view_name[0]} LIMIT 5")
                                            test_data = test_cursor.fetchall()
                                            test_cols = [desc[0] for desc in test_cursor.description]
                                            test_cursor.close()
                                            test_conn.close()
                                            
                                            if test_data:
                                                import pandas as pd
                                                st.success(f"✅ Found {len(test_data)} row(s)")
                                                st.dataframe(pd.DataFrame(test_data, columns=test_cols))
                                            else:
                                                st.warning("⚠️ VIEW exists but returned no data")
                                        except Exception as e:
                                            st.error(f"❌ Test failed: {e}")
                
                # Show errors - PERSISTENT
                if result.get('errors'):
                    st.markdown("### ❌ Errors During VIEW Creation:")
                    for idx, error in enumerate(result['errors']):
                        st.session_state.debug_logs.append(f"[{datetime.now()}] ERROR {idx+1}: {error}")
                        with st.expander(f"Error {idx+1}", expanded=True):
                            st.error(error)
                
            except Exception as e:
                st.session_state.debug_logs.append(f"[{datetime.now()}] FATAL ERROR: {str(e)}")
                st.error(f"❌ VIEW generation failed: {e}")
                
                with st.expander("🔍 Full Error Trace", expanded=True):
                    import traceback
                    error_trace = traceback.format_exc()
                    st.code(error_trace)
                    st.session_state.debug_logs.append(f"[{datetime.now()}] Traceback: {error_trace}")

                
                st.rerun()
    
    # ========== Footer ==========
    st.markdown("---")
    st.caption("ETL Mapping Generator v2.0 | © 2025")






# TAB 4: Monitor & Reports
with tab4:
    st.header("📊 Step 4: Monitoring & Reports")
    
    try:
        st.subheader("📈 Execution History")
        history = db_helper.load_execution_history(limit=20)
        if not history.empty:
            st.dataframe(history, use_container_width=True)
        else:
            st.info("No execution history yet")
        
        st.markdown("---")
        st.subheader("🔍 Reconciliation Results")
        recon = db_helper.load_reconciliation_results(limit=10)
        if not recon.empty:
            st.dataframe(recon, use_container_width=True)
        else:
            st.info("No reconciliation data yet")
    except Exception as e:
        st.error(f"Error: {e}")

st.markdown("---")
st.markdown("<div style='text-align: center; color: gray;'>ETL Mapping Generator v2.0 | © 2025</div>", unsafe_allow_html=True)
