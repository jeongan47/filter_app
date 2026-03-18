import streamlit as st
import pandas as pd
import re
from io import StringIO, BytesIO

# 1. 개별 아이템 분류 함수
def classify_to_list(item_list):
    result = {"번호": [], "이메일": [], "url": []}
    words_to_remove = ["연락처", "및", "사이트:", "이메일:", "사이트", "이메일"]
    if not isinstance(item_list, list): return pd.Series(result)
    for item in item_list:
        item = str(item).strip()
        if not item or item.lower() == 'nan' or item in words_to_remove: continue
        if "@" in item: result["이메일"].append(item)
        elif re.search(r'\d{2,4}-\d{3,4}', item): result["번호"].append(item)
        else: result["url"].append(item)
    return pd.Series({
        "번호": ", ".join(result["번호"]) if result["번호"] else None,
        "이메일": ", ".join(result["이메일"]) if result["이메일"] else None,
        "url": ", ".join(result["url"]) if result["url"] else None
    })

# 2. 파일 읽기 전용 함수 (에러 방지 핵심)
def flexible_read_excel(file):
    try:
        # 시도 1: 표준 엑셀로 읽기
        return pd.read_excel(file)
    except Exception:
        try:
            # 시도 2: HTML/StringIO 방식으로 읽기 (구형 .xls 대응)
            file.seek(0)
            content = file.read().decode('utf-8', errors='ignore')
            df_list = pd.read_html(StringIO(content))
            if df_list:
                df = df_list[0]
                # 첫 줄이 컬럼명이 아닐 경우를 대비한 처리
                if df.columns.dtype == 'int':
                    df.columns = df.iloc[0]
                    df = df[1:].reset_index(drop=True)
                return df
        except Exception as e:
            st.error(f"파일 구조를 해석할 수 없습니다: {e}")
            return None

def process_pipeline(file):
    df = flexible_read_excel(file)
    if df is not None and "사이트주소" in df.columns:
        df["분리"] = df["사이트주소"].astype(str).str.split(" ")
        new_cols = df["분리"].apply(classify_to_list)
        df = pd.concat([df, new_cols], axis=1)
        cols_to_drop = ["매체사", "사이트주소", "분리", "등록자", "등록일"]
        df = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors='ignore')
    return df

def filter_data(target_df, blacklist_df):
    def get_unique_values(df, col_name):
        if col_name not in df.columns: return set()
        all_vals = df[col_name].dropna().astype(str).str.split(", ")
        return set([item for sublist in all_vals for item in sublist if item])

    black_phones = get_unique_values(blacklist_df, "번호")
    black_emails = get_unique_values(blacklist_df, "이메일")
    black_urls = get_unique_values(blacklist_df, "url")

    def check_is_black(row):
        for col, black_set in [("번호", black_phones), ("이메일", black_emails), ("url", black_urls)]:
            if col in row and pd.notna(row[col]):
                items = str(row[col]).split(", ")
                if any(it in black_set for it in items): return True
        return False

    target_df["is_black"] = target_df.apply(check_is_black, axis=1)
    clean_df = target_df[target_df["is_black"] == False].copy()
    banned_df = target_df[target_df["is_black"] == True].copy()
    return clean_df.drop(columns=["is_black"]), banned_df.drop(columns=["is_black"])

def to_excel(df):
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False)
    return output.getvalue()

# --- 화면 구성 ---
st.title("필터링 서비스 (에러 대응 버전)")

col1, col2 = st.columns(2)
with col1:
    blacklist_file = st.file_uploader("영업금지리스트(.xls)", type=["xls", "xlsx"])
with col2:
    target_file = st.file_uploader("검증대상데이터(.xlsx)", type=["xlsx"])

if blacklist_file and target_file:
    if st.button("🚀 실행"):
        blacklist_df = process_pipeline(blacklist_file)
        target_df_raw = pd.read_excel(target_file)
        
        # 검증 대상 데이터도 파이프라인 적용
        if "사이트주소" in target_df_raw.columns:
            target_df_raw["분리"] = target_df_raw["사이트주소"].astype(str).str.split(" ")
            target_df = pd.concat([target_df_raw, target_df_raw["분리"].apply(classify_to_list)], axis=1)
            target_df.drop(columns=["분리", "사이트주소"], inplace=True, errors='ignore')
        else:
            target_df = target_df_raw

        if blacklist_df is not None:
            clean, banned = filter_data(target_df, blacklist_df)
            st.success(f"완료! (안전: {len(clean)} / 차단: {len(banned)})")
            st.download_button("📥 안전 결과 다운로드", to_excel(clean), "safe.xlsx")
            st.download_button("📥 차단 결과 다운로드", to_excel(banned), "banned.xlsx")