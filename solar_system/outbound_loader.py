from .data_loader import DataLoader
import pandas as pd

class OutboundDataLoader(DataLoader):
    """
    A specific data loader for 'outbound' data, inheriting from DataLoader.
    It applies specific filters relevant to the outbound dataset.
    """
    def __init__(self, config: dict, global_config: dict):
        """
        Initializes the OutboundDataLoader.

        Args:
            config (dict): Configuration for the 'outbound' dataset.
            global_config (dict): Global column configurations.
        """
        super().__init__(config, global_config)

    def apply_specific_filters(self):
        mask = (
            self.df['MaPhieuDeXuat'].notnull()
            & ~self.df["MaKho"].str.contains("GTEX01|HP01|HPKGHCM|HPKGHNI|^KK|KIEMKE|ONL|KHOSG", na=False)
            & ~self.df["LoaiPhieu"].str.contains("Xuất tạo Combo|Xuất hủy Combo", na=False)
            & ~(
                (self.df["LoaiPhieu"] == 'Xuất khác') & self.df["DienGiai"].str.lower().str.contains("điều chuyển|chênh lệch|ddhh|sticker|thiếu cont|ycdg|ycđg|yêu cầu đóng gói|yêu cầu rã|yêu cầu xả|combo|xử lý số liệu chênh lệch", na=False)
            )
        )
        self.df = self.df[mask]
        print('Outbound applied filter')
        return self.df
    
    def merge_ref(self, dmsp_df: pd.DataFrame):
        # Keep only relevant columns from dmsp_df for merging
        # Assuming you want 'TenSanPham' and 'DonVi' from DMSP
        dmsp_cols_to_merge = ['MaSanPham', 'CBM_Unit']
        
        # Filter dmsp_df_for_merge to only include columns that actually exist in dmsp_df
        existing_dmsp_cols = [col for col in dmsp_cols_to_merge if col in dmsp_df.columns]
        if 'MaSanPham' not in existing_dmsp_cols:
            print("Error: 'MaSanPham' is missing from DMSP data for merge. Skipping merge.")
            return

        dmsp_df_for_merge = dmsp_df[existing_dmsp_cols]

        # Perform the left merge
        # The result updates the self.df of OutboundDataLoader
        self.df = pd.merge(self.df, dmsp_df_for_merge, on='MaSanPham', how='left')
        print(f"Outbound merged to DMSP")

    def _calculate(self):
        # Calculate totalCBM
        self.df['TotalCBM'] = self.df['SoLuong'] * self.df['CBM_Unit']

        # Calculate month from NgayPhieu
        self.df['Month'] = self.df['NgayPhieu'].dt.strftime('%Y-%m')
        
        print("Columns calculation finished.")

    def process_data(self, dmsp_df: pd.DataFrame = None):
        """
        Overrides the base process_data to include specific filtering after transformations.
        """
        super().process_data() # Call base class processing first
        if not self.df.empty:
            self.apply_specific_filters()
            if dmsp_df is not None:
                self.merge_ref(dmsp_df)
            self._calculate()
        return self.df
