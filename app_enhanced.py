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
                    xml_id = db_helper.save_xml_to_stage_with_copy(
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


# # TAB 3: Execute ETL
# with tab3:
#     st.header("⚡ Step 3: Execute ETL to Silver Layer")
    
#     try:
#         approved_mappings = db_helper.load_approved_mappings()
        
#         # ✅ Check if DataFrame is empty OR missing xml_id column
#         if approved_mappings.empty:
#             st.info("ℹ️ No approved mappings. Approve mappings in Step 2 first.")
#         elif 'xml_id' not in approved_mappings.columns:
#             st.error("❌ Database schema issue: 'xml_id' column missing from approved mappings.")
#             st.warning("💡 Check that `load_approved_mappings()` in database_helper.py includes xml_id in the SELECT query.")
            
#             # Debug: Show what columns ARE present
#             with st.expander("🔍 Debug: Available Columns"):
#                 st.write("Columns returned:", list(approved_mappings.columns))
#                 st.dataframe(approved_mappings.head())
#         else:
#             xml_ids = approved_mappings['xml_id'].unique()
            
#             st.info(f"📊 Found {len(approved_mappings)} approved mappings for {len(xml_ids)} XML file(s)")
            
#             selected_xml = st.selectbox(
#                 "Select XML to execute:",
#                 xml_ids,
#                 format_func=lambda x: f"{x} ({len(approved_mappings[approved_mappings['xml_id']==x])} mappings)"
#             )
            
#             xml_mappings = approved_mappings[approved_mappings['xml_id'] == selected_xml]
            
#             st.subheader(f"📋 Mappings for {selected_xml}")
#             st.dataframe(xml_mappings[['source_node', 'target_table', 'target_column', 'transformation_logic', 'confidence_score']], 
#                         use_container_width=True, height=300)
            
#             # Show summary
#             tables = xml_mappings['target_table'].unique()
#             st.markdown(f"**Target Tables:** {', '.join(tables)}")
            
#             col1, col2 = st.columns([1, 3])
            
#             with col1:
#                 if st.button("🚀 Execute ETL", type="primary"):
#                     with st.spinner("⏳ Executing ETL..."):
#                         try:
#                             summary = etl_executor.execute_mappings(selected_xml, xml_mappings)
                            
#                             st.success(f"""
#                             ✅ **ETL Execution Complete!**
#                             - Execution ID: `{summary['execution_id']}`
#                             - Tables Processed: {summary['tables_processed']}
#                             - Total Rows Inserted: {summary['total_rows']}
#                             """)
                            
#                             if summary['successful_tables']:
#                                 st.write("**✅ Successful Tables:**")
#                                 for table in summary['successful_tables']:
#                                     st.write(f"  - {table}")
                            
#                             if summary['failed_tables']:
#                                 st.error("**❌ Failed Tables:**")
#                                 for i, table in enumerate(summary['failed_tables']):
#                                     st.write(f"  - {table}: {summary['errors'][i]}")
                            
#                             st.balloons()
#                             st.rerun()
                            
#                         except Exception as e:
#                             st.error(f"❌ Execution failed: {e}")
#                             with st.expander("🔍 Error Details"):
#                                 import traceback
#                                 st.code(traceback.format_exc())
            
#             with col2:
#                 st.info("💡 This will insert data from staging tables into Silver layer tables using the approved mappings.")
            
#     except Exception as e:
#         st.error(f"Error: {e}")
#         with st.expander("🔍 Debug"):
#             import traceback
#             st.code(traceback.format_exc())

# Initialize session state for debug logs
if 'debug_logs' not in st.session_state:
    st.session_state.debug_logs = []
if 'last_execution_summary' not in st.session_state:
    st.session_state.last_execution_summary = None

# TAB 3: Execute ETL
with tab3:
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
