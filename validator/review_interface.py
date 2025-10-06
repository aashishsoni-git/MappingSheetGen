# validator/review_interface.py
import streamlit as st

class MappingReviewer:
    def create_review_ui(self, predictions_df):
        """Create Streamlit UI for reviewing predictions"""
        st.title("ETL Mapping Review")
        
        for idx, row in predictions_df.iterrows():
            with st.expander(f"{row['source_node']} → {row['target_column']}"):
                st.metric("Confidence", f"{row['confidence_score']:.2%}")
                st.write(f"**Reasoning:** {row['reasoning']}")
                st.text_area("Transformation Logic", row['transformation_logic'])
                
                col1, col2 = st.columns(2)
                approved = col1.button("Approve", key=f"approve_{idx}")
                rejected = col2.button("Reject", key=f"reject_{idx}")
                
                if approved:
                    self._save_feedback(row, "approved")
                if rejected:
                    corrected = st.text_input("Correct mapping", key=f"correct_{idx}")
                    if corrected:
                        self._save_feedback(row, "rejected", corrected)
