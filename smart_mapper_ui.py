# smart_mapper_ui.py
import streamlit as st
import pandas as pd
import os
from pathlib import Path
import xml.etree.ElementTree as ET
from extractors.xml_parser import XMLMetadataExtractor
from loaders.snowflake_loader import SnowflakeStageLoader
from mapper.openai_mapper import AIETLMapper
from utils.logging_config import setup_logging
from utils.cost_estimator import estimate_mapping_cost
from dotenv import load_dotenv
import logging

load_dotenv()
setup_logging(log_level='INFO')
logger = logging.getLogger(__name__)

st.set_page_config(
    page_title="Smart ETL Mapping Generator",
    page_icon="🎯",
    layout="wide"
)

# Product detection keywords
PRODUCT_KEYWORDS = {
    'PA001': ['PersonalAuto', 'Auto', 'Vehicle', 'Car', 'Driver', 'VIN'],
    'HO003': ['Homeowners', 'Property', 'Dwelling', 'Residence', 'Building'],
    'CP001': ['Commercial', 'BusinessProperty', 'Office', 'CommercialBuilding'],
    'UMB001': ['Umbrella', 'ExcessLiability', 'PersonalLiability'],
    'WC001': ['WorkersComp', 'WorkersCompensation', 'Employee']
}

PRODUCT_NAMES = {
    'PA001': 'Personal Auto Insurance',
    'HO003': 'Homeowners Insurance',
    'CP001': 'Commercial Property Insurance',
    'UMB001': 'Personal Umbrella Insurance',
    'WC001': 'Workers Compensation'
}

def detect_product_from_xml(xml_file):
    """
    Intelligently detect product type from XML content
    """
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        xml_content = ET.tostring(root, encoding='unicode').lower()
        
        # Score each product based on keyword matches
        product_scores = {}
        for product_code, keywords in PRODUCT_KEYWORDS.items():
            score = sum(1 for keyword in keywords if keyword.lower() in xml_content)
            product_scores[product_code] = score
        
        # Get product with highest score
        detected_product = max(product_scores, key=product_scores.get)
        confidence = product_scores[detected_product]
        
        return detected_product, confidence, product_scores
    except Exception as e:
        logger.error(f"Error detecting product: {str(e)}")
        return None, 0, {}

def load_product_specific_mappings(product_code):
    """
    Load only relevant historical mappings for detected product
    """
    mapping_files = {
        'PA001': 'reference_data/old_mappings_personal_auto.csv',
        'HO003': 'reference_data/old_mappings_homeowners.csv',
        'CP001': 'reference_data/old_mappings_commercial.csv'
    }
    
    # Try product-specific mapping first
    if product_code in mapping_files:
        specific_file = mapping_files[product_code]
        if os.path.exists(specific_file):
            df = pd.read_csv(specific_file)
            logger.info(f"Loaded {len(df)} mappings for product {product_code}")
            return df
    
    # Fallback to combined mappings filtered by product
    if os.path.exists('reference_data/old_mappings.csv'):
        df = pd.read_csv('reference_data/old_mappings.csv')
        if 'product_code' in df.columns:
            filtered = df[df['product_code'] == product_code]
            if not filtered.empty:
                logger.info(f"Loaded {len(filtered)} filtered mappings for {product_code}")
                return filtered
        return df
    
    return pd.DataFrame()

def save_uploaded_xml(uploaded_file, product_code):
    """
    Save uploaded XML to data directory with product-specific naming
    """
    os.makedirs('data/uploads', exist_ok=True)
    timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
    filename = f"data/uploads/{product_code}_{timestamp}_{uploaded_file.name}"
    
    with open(filename, 'wb') as f:
        f.write(uploaded_file.getbuffer())
    
    logger.info(f"Saved uploaded file to {filename}")
    return filename

# Main UI
st.title("🎯 Smart ETL Mapping Generator")
st.markdown("### Upload XML and automatically generate data mappings")

# Sidebar configuration
with st.sidebar:
    st.header("⚙️ Configuration")
    
    # Check if credentials are configured
    openai_configured = bool(os.getenv('OPENAI_API_KEY'))
    snowflake_configured = bool(os.getenv('SF_ACCOUNT'))
    
    st.metric("OpenAI Status", "✅ Configured" if openai_configured else "❌ Not Configured")
    st.metric("Snowflake Status", "✅ Configured" if snowflake_configured else "❌ Not Configured")
    
    st.markdown("---")
    
    # Advanced options
    with st.expander("Advanced Options"):
        auto_detect = st.checkbox("Auto-detect product type", value=True)
        load_to_snowflake = st.checkbox("Load XML to Snowflake", value=True)
        generate_mappings = st.checkbox("Generate AI mappings", value=True)

# Main content area
tab1, tab2, tab3, tab4 = st.tabs(["📤 Upload XML", "🔍 Review Mappings", "📊 Statistics", "📚 Reference Data"])

with tab1:
    st.header("Upload XML Transaction File")
    
    # File uploader
    uploaded_file = st.file_uploader(
        "Choose an XML file",
        type=['xml'],
        help="Upload DuckCreek transaction XML file (Policy, Quote, etc.)"
    )
    
    if uploaded_file is not None:
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.success(f"✅ File uploaded: {uploaded_file.name}")
            st.info(f"📦 File size: {uploaded_file.size / 1024:.2f} KB")
            
            # Preview XML
            with st.expander("🔍 Preview XML Content"):
                try:
                    xml_content = uploaded_file.getvalue().decode('utf-8')
                    st.code(xml_content[:2000] + "..." if len(xml_content) > 2000 else xml_content, language='xml')
                except Exception as e:
                    st.error(f"Error reading XML: {str(e)}")
        
        with col2:
            st.subheader("Product Detection")
            
            # Reset file pointer for detection
            uploaded_file.seek(0)
            
            if auto_detect:
                with st.spinner("🔍 Detecting product type..."):
                    detected_product, confidence, scores = detect_product_from_xml(uploaded_file)
                    uploaded_file.seek(0)  # Reset again
                    
                    if detected_product and confidence > 0:
                        st.success(f"**Detected:** {PRODUCT_NAMES.get(detected_product, detected_product)}")
                        st.metric("Confidence", f"{confidence} keywords matched")
                        
                        with st.expander("Detection Details"):
                            for product, score in sorted(scores.items(), key=lambda x: x[1], reverse=True):
                                st.write(f"{PRODUCT_NAMES.get(product, product)}: {score} matches")
                        
                        selected_product = st.selectbox(
                            "Confirm or change product:",
                            options=list(PRODUCT_NAMES.keys()),
                            format_func=lambda x: PRODUCT_NAMES[x],
                            index=list(PRODUCT_NAMES.keys()).index(detected_product)
                        )
                    else:
                        st.warning("⚠️ Could not auto-detect product")
                        selected_product = st.selectbox(
                            "Select product manually:",
                            options=list(PRODUCT_NAMES.keys()),
                            format_func=lambda x: PRODUCT_NAMES[x]
                        )
            else:
                selected_product = st.selectbox(
                    "Select product type:",
                    options=list(PRODUCT_NAMES.keys()),
                    format_func=lambda x: PRODUCT_NAMES[x]
                )
        
        st.markdown("---")
        
        # Process button
        if st.button("🚀 Process and Generate Mappings", type="primary", use_container_width=True):
            
            # Save uploaded file
            uploaded_file.seek(0)
            saved_filepath = save_uploaded_xml(uploaded_file, selected_product)
            
            # Create processing status
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            try:
                # Step 1: Extract XML Metadata (20%)
                status_text.text("📊 Extracting XML metadata...")
                progress_bar.progress(20)
                
                xml_parser = XMLMetadataExtractor()
                xml_metadata = xml_parser.extract_schema(saved_filepath)
                
                st.success(f"✅ Extracted {len(xml_metadata)} XML nodes")
                
                # Step 2: Load to Snowflake (40%)
                if load_to_snowflake and snowflake_configured:
                    status_text.text("☁️ Loading to Snowflake...")
                    progress_bar.progress(40)
                    
                    sf_config = {
                        'account': os.getenv('SF_ACCOUNT'),
                        'user': os.getenv('SF_USER'),
                        'password': os.getenv('SF_PASSWORD'),
                        'warehouse': os.getenv('SF_WAREHOUSE'),
                        'database': os.getenv('SF_DATABASE'),
                        'schema': os.getenv('SF_SCHEMA'),
                        'role': os.getenv('SF_ROLE')
                    }
                    
                    sf_loader = SnowflakeStageLoader(**sf_config)
                    table_name = f"STAGE_XML_{selected_product}"
                    sf_loader.load_xml_as_json(saved_filepath, table_name)
                    sf_loader.close()
                    
                    st.success(f"✅ Loaded to Snowflake table: {table_name}")
                
                # Step 3: Load Product-Specific References (60%)
                status_text.text("📚 Loading reference mappings...")
                progress_bar.progress(60)
                
                hist_mappings = load_product_specific_mappings(selected_product)
                data_dict = pd.read_csv('reference_data/data_dictionary.csv')
                
                st.success(f"✅ Loaded {len(hist_mappings)} historical mappings for {PRODUCT_NAMES[selected_product]}")
                
                # Step 4: Get Silver Schema (70%)
                status_text.text("🗄️ Fetching Silver layer schema...")
                progress_bar.progress(70)
                
                # Sample schema (replace with actual Snowflake query if connected)
                silver_schema = pd.DataFrame({
                    'table_name': ['SILVER.POLICY'] * 5 + ['SILVER.COVERAGE'] * 3,
                    'column_name': ['policy_id', 'policy_number', 'product_code', 'effective_date', 'premium_amount',
                                   'coverage_id', 'coverage_code', 'coverage_limit'],
                    'data_type': ['VARCHAR(50)', 'VARCHAR(100)', 'VARCHAR(50)', 'DATE', 'DECIMAL(15,2)',
                                 'VARCHAR(50)', 'VARCHAR(50)', 'DECIMAL(15,2)'],
                    'description': ['Unique policy identifier', 'Policy number', 'Product code', 'Effective date', 'Premium amount',
                                   'Coverage identifier', 'Coverage code', 'Coverage limit']
                })
                
                # Step 5: Estimate Cost (80%)
                status_text.text("💰 Estimating AI costs...")
                progress_bar.progress(80)
                
                estimated_cost = estimate_mapping_cost(
                    len(xml_metadata),
                    len(silver_schema),
                    len(hist_mappings)
                )
                
                st.info(f"💵 Estimated OpenAI API cost: ${estimated_cost:.4f}")
                
                # Step 6: Generate Mappings (90%)
                if generate_mappings and openai_configured:
                    status_text.text("🤖 Generating AI-powered mappings...")
                    progress_bar.progress(90)
                    
                    ai_mapper = AIETLMapper(os.getenv('OPENAI_API_KEY'))
                    predictions = ai_mapper.predict_mappings(
                        xml_metadata,
                        silver_schema,
                        hist_mappings,
                        data_dict
                    )
                    
                    # Save results
                    os.makedirs('output', exist_ok=True)
                    output_file = f'output/mappings_{selected_product}_{pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")}.csv'
                    
                    mappings_df = pd.DataFrame([m.dict() for m in predictions.mappings])
                    mappings_df.to_csv(output_file, index=False)
                    
                    # Store in session state for review
                    st.session_state['mappings_df'] = mappings_df
                    st.session_state['product_code'] = selected_product
                    st.session_state['xml_file'] = uploaded_file.name
                    
                    progress_bar.progress(100)
                    status_text.text("✅ Complete!")
                    
                    # Display summary
                    st.success("🎉 Mapping generation complete!")
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Total Mappings", len(mappings_df))
                    with col2:
                        st.metric("Avg Confidence", f"{mappings_df['confidence_score'].mean():.1%}")
                    with col3:
                        high_conf = len(mappings_df[mappings_df['confidence_score'] >= 0.8])
                        st.metric("High Confidence", f"{high_conf}/{len(mappings_df)}")
                    
                    # Download button
                    csv = mappings_df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="📥 Download Mapping Sheet (CSV)",
                        data=csv,
                        file_name=f"mappings_{selected_product}_{uploaded_file.name}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
                    
                    st.info("💡 Switch to 'Review Mappings' tab to see detailed results")
                    
                else:
                    progress_bar.progress(100)
                    status_text.text("✅ Processing complete (AI mapping generation skipped)")
                
            except Exception as e:
                st.error(f"❌ Error during processing: {str(e)}")
                logger.error(f"Processing error: {str(e)}", exc_info=True)

with tab2:
    st.header("🔍 Review Generated Mappings")
    
    if 'mappings_df' in st.session_state:
        mappings_df = st.session_state['mappings_df']
        
        st.success(f"✅ Showing mappings for: {st.session_state.get('xml_file', 'Unknown')}")
        st.info(f"📦 Product: {PRODUCT_NAMES.get(st.session_state.get('product_code'), 'Unknown')}")
        
        # Filter options
        col1, col2 = st.columns(2)
        with col1:
            min_confidence = st.slider("Minimum Confidence", 0.0, 1.0, 0.0, 0.05)
        with col2:
            target_table = st.multiselect(
                "Filter by Target Table",
                options=mappings_df['target_table'].unique().tolist(),
                default=mappings_df['target_table'].unique().tolist()
            )
        
        # Apply filters
        filtered_df = mappings_df[
            (mappings_df['confidence_score'] >= min_confidence) &
            (mappings_df['target_table'].isin(target_table))
        ]
        
        # Display mappings
        st.dataframe(
            filtered_df,
            use_container_width=True,
            column_config={
                "confidence_score": st.column_config.ProgressColumn(
                    "Confidence",
                    format="%.1f%%",
                    min_value=0,
                    max_value=1,
                ),
            }
        )
        
        # Detailed view
        st.subheader("Detailed Mapping View")
        for idx, row in filtered_df.iterrows():
            with st.expander(f"{row['source_node']} → {row['target_column']} ({row['confidence_score']:.1%})"):
                col1, col2 = st.columns(2)
                with col1:
                    st.write("**Source:**")
                    st.code(row['source_node'])
                    if pd.notna(row.get('source_attribute')):
                        st.write(f"Attribute: `{row['source_attribute']}`")
                
                with col2:
                    st.write("**Target:**")
                    st.code(f"{row['target_table']}.{row['target_column']}")
                
                if pd.notna(row.get('transformation_logic')):
                    st.write("**Transformation:**")
                    st.code(row['transformation_logic'], language='sql')
                
                st.write("**Reasoning:**")
                st.info(row['reasoning'])
                
                # Approval buttons
                col1, col2, col3 = st.columns(3)
                with col1:
                    if st.button("✅ Approve", key=f"approve_{idx}"):
                        st.success("Approved!")
                with col2:
                    if st.button("✏️ Edit", key=f"edit_{idx}"):
                        st.info("Edit functionality coming soon!")
                with col3:
                    if st.button("❌ Reject", key=f"reject_{idx}"):
                        st.warning("Rejected!")
    else:
        st.info("📤 Upload and process an XML file first to see mappings here")

with tab3:
    st.header("📊 Mapping Statistics")
    
    if 'mappings_df' in st.session_state:
        mappings_df = st.session_state['mappings_df']
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Mappings", len(mappings_df))
        with col2:
            st.metric("Average Confidence", f"{mappings_df['confidence_score'].mean():.1%}")
        with col3:
            high_conf = len(mappings_df[mappings_df['confidence_score'] >= 0.8])
            st.metric("High Confidence (≥80%)", high_conf)
        with col4:
            low_conf = len(mappings_df[mappings_df['confidence_score'] < 0.5])
            st.metric("Low Confidence (<50%)", low_conf)
        
        # Distribution chart
        st.subheader("Confidence Score Distribution")
        st.bar_chart(mappings_df['confidence_score'].value_counts().sort_index())
        
        # Table breakdown
        st.subheader("Mappings by Target Table")
        table_counts = mappings_df['target_table'].value_counts()
        st
