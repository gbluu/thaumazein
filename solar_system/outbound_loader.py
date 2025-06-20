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
            & ~self.df["LoaiPhieu"].str.contains("Xuất tạo Combo|Xuất hủy Combo|Xuất hủy", na=False)
            & ~(
                (self.df["LoaiPhieu"] == 'Xuất khác') & self.df["DienGiai"].str.lower().str.contains("điều chuyển|chênh lệch|ddhh|sticker|thiếu cont|ycdg|ycđg|yêu cầu đóng gói|yêu cầu rã|yêu cầu xả|combo|xử lý số liệu chênh lệch", na=False)
            )
        )
        self.df = self.df[mask]
        print('Outbound applied filter')
        return self.df
    
    def merge_ref(self, dmsp_df: pd.DataFrame, whs_df: pd.DataFrame):
        # Keep only relevant columns 
        dmsp_cols_to_merge = ['MaSanPham', 'CBM_Unit']
        whs_cols_to_merge = ['MaKho', 'TenKho', 'KhoGop', 'Mien']

        dmsp_df_for_merge = dmsp_df[dmsp_cols_to_merge]
        whs_df_for_merge = whs_df[whs_cols_to_merge]

        # Perform the left merge
        self.df = pd.merge(self.df, dmsp_df_for_merge, on='MaSanPham', how='left')
        self.df = pd.merge(self.df, whs_df_for_merge, on='MaKho', how='left')
        print(f"Outbound merged to refs")

    def _calculate(self):
        # Calculate totalCBM
        self.df['TotalCBM'] = self.df['SoLuong'] * self.df['CBM_Unit']

        # Calculate month from NgayPhieu
        self.df['Month'] = self.df['NgayPhieu'].dt.strftime('%Y-%m')
        
        print("Columns calculation finished.")

    def process_data(self, dmsp_df: pd.DataFrame = None, whs_df: pd.DataFrame = None):
        """
        Overrides the base process_data to include specific filtering after transformations.
        """
        super().process_data() # Call base class processing first
        if not self.df.empty:
            self.apply_specific_filters()
            self.merge_ref(dmsp_df, whs_df)
            self._calculate()
        return self.df
