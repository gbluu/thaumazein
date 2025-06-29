# Global column configuration
global_config = {
    "rename_columns": {
        "Số\nTT": "Stt",
        "Loại phiếu": "LoaiPhieu",
        "Ngày phiếu xuất": "NgayPhieu",
        "Mã phiếu xuất": "MaPhieu",
        "Mã phiếu đề xuất": "MaPhieuDeXuat",
        "Ngày hóa đơn": "NgayHoaDon",
        "Số hóa đơn": "SoHoaDon",
        "Mã KH / NCC\n(Customer Code / Vendor Code)": "MaKhachHang",
        "Tên KH 2 / NCC": "TenCuaHang",
        "Tên KH / NCC\n(Customer Name / Vendor Name)": "TenCongTy",
        "Mã chủ hàng": "MaChuHang",
        "Tên chủ hàng": "TenChuHang",
        "Chi nhánh": "ChiNhanh",
        "Diễn giải\n(Description)": "DienGiai",
        "Mã vật tư\n(Goods Code)": "MaSanPham",
        "BarCode": "Barcode",
        "Tên vật tư\n(Goods Name)": "TenSanPham",
        "Tên tiếng Anh\nEnglish Name)": "TenTiengAnh",
        "Đơn vị\n(Unit)": "DonVi",
        "Số lượng\n(Quantily)": "SoLuong",
        "Giá bán\n(Price)": "GiaBan",
        "Tiền hàng\n(Sub Total)": "TienHang",
        "Tiền CK\n(Discount)": "ChietKhau",
        "Tiền thuế\n(VAT)": "Thue",
        "Tồng tiền tt\n(Grand Total)": "TongTien",
        "Giá vốn\n(Cost Price)": "GiaVon",
        "Tiền vốn\n(Capital)": "TienVon",
        "Mã kho\n(Warehouse Code)": "MaKho",
        "Nhóm vật tư chính (Goods KeyGroup)": "Brand",
        "Nhóm vật tư phụ 1 - (Goods SubGroup1)": "SubBrand1",
        "Nhóm vật tư phụ 2 - (Goods SubGroup2)": "SubBrand2",
        "Nhóm vật tư phụ 3 - (Goods SubGroup3)": "SubBrand3",
        "LOT": "LOT",
        "HSD": "HanSuDungTheoLo",
        "Ngày SX (Manufactur Date)": "NgaySanXuat",
        "Ngày hết hạn (Expiry Date)": "HanSuDung",
        "Số đơn đặt hàng": "SoDonDatHang",
        "Ngày đặt hàng": "NgayDatHang",
        "PRODUCT CODE": "MaSanPham",
        "PacksPerCase": "PacksPerCase",
        "CBM/Unit": "CBM_Unit",
        "GrossWeightProductg": "WeightG",
        " Shelf life\n(Day) ": "ShelfLife"

    },
    "numeric_columns": [
        "SoLuong",
        # "GiaBan",
        # "TienHang",
        # "ChietKhau",
        # "Thue",
        # "TongTien",
        # "GiaVon",
        # "TienVon",
        "CBM_Unit"
    ],
    "datetime_columns": [
        "NgayPhieu",
    ]
}

# Dataset-specific configurations

outbound_config = {
    "type": "folder",
    "path": "nebula/outbound",
    "header_row": 4,
    "keep_columns": [
        "NgayPhieu",
        "MaPhieu",
        "MaPhieuDeXuat",
        "LoaiPhieu",
        # "NgayHoaDon",
        # "SoHoaDon",
        # "MaKhachHang",
        # "TenCuaHang",
        # "TenCongTy",
        # "MaChuHang",
        # "TenChuHang",
        # "ChiNhanh",
        "DienGiai",
        "MaSanPham",
        "HanSuDung",
        "Brand",
        "MaKho",
        # "Barcode",
        # "TenSanPham",
        # "TenTiengAnh",
        # "DonVi",
        "SoLuong",
        # "GiaBan",
        # "TienHang",
        # "ChietKhau",
        # "Thue",
        # "TongTien",
        # "GiaVon",
        # "TienVon",
        # "SubBrand1",
        # "SubBrand2",
        # "SubBrand3",
        # "LOT",
        # "HanSuDungTheoLo",
        # "NgaySanXuat",
        # "SoDonDatHang",
        # "NgayDatHang"
        ],
}

dmsp_config = {
    "type": "file",
    "path": "nebula/ref/dmsp.csv",
    "header_row": 4,
    "keep_columns": [
        "MaSanPham",
        "CBM_Unit",
        "PacksPerCase",
        "WeightG",
        "ShelfLife"
    ]
}

whs_config = {
    "type": "file",
    "path": "nebula/ref/codekho.csv",
    "header_row": 0,
    "keep_columns": [
        'MaKho',
        'TenKho',
        'KhoGop',
        'Mien',
        'LoaiKho',
    ]
}