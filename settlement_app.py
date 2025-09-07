import streamlit as st
import pandas as pd
import json
import io
from datetime import datetime
import traceback

# --- 1. 설정 및 공통 함수 ---

@st.cache_data
def load_config(path='config.json'):
    """설정 파일을 불러옵니다."""
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        st.error(f"설정 파일({path})을 찾을 수 없습니다. 파일이 정확한 위치에 있는지 확인해 주세요.")
        st.stop()

CONFIG = load_config()

def filter_by_previous_month(df, date_col):
    """데이터프레임을 이전 달 기준으로 필터링합니다."""
    if date_col not in df.columns or df.empty:
        return pd.DataFrame()
    
    # 마지막 행(요약행)이 있을 수 있으므로 iloc[:-1] 사용
    if len(df) > 1:
        df = df.iloc[:-1].copy()
        
    df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
    df.dropna(subset=[date_col], inplace=True)
    if df.empty:
        return pd.DataFrame()

    today = pd.Timestamp.now()
    prev_month_year = today.year if today.month > 1 else today.year - 1
    prev_month = today.month - 1 if today.month > 1 else 12
    return df[(df[date_col].dt.year == prev_month_year) & (df[date_col].dt.month == prev_month)].copy()


# --- 2. 데이터 처리 함수 ---

def process_shipping_data(df):
    """출고 데이터를 처리합니다."""
    config = CONFIG['rules']['shipping']
    if df.empty: return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    df = filter_by_previous_month(df, config['date_col'])
    if df.empty: return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    df['[브랜드]'] = df['[브랜드]'].astype(str).str.split(':').str[0].str.strip()
    df = df[~df['[브랜드]'].isin(CONFIG['general']['excluded_brands'])].copy()

    for col, val in config['filters'].items():
        if col.startswith('_'): continue
        if col.endswith('_exclude'):
            df = df[df[col.replace('_exclude', '')] != val].copy()
        elif isinstance(val, list):
            df = df[~df[col].isin(val)].copy()
        else:
            df = df[df[col] == val].copy()

    df['[매출처]'] = df['[매출처]'].astype(str).apply(lambda x: x.split(': ', 1)[1] if ': ' in x else x)

    type_col, mall_col = '[택배사]', '[매출처]'
    cond_type_normal = df[type_col].isin(config['type_conditions']['normal_type'])
    cond_mall_keywords = df[mall_col].str.contains('|'.join(config['type_conditions']['normal_mall_keywords']), na=False)
    is_normal = ((df[type_col] == config['type_conditions']['normal_type'][0]) & cond_mall_keywords) | cond_type_normal
    
    df_normal_candidate = df[is_normal]
    df_shipping_type_abnormal = df[~is_normal]
    
    df_main = df_normal_candidate[df_normal_candidate[mall_col].isin(config['mall_list'])]
    df_store_abnormal = df_normal_candidate[~df_normal_candidate[mall_col].isin(config['mall_list'])]

    def assign_category(d, category_map, seeding_map):
        if d.empty: return d
        d = d.copy()
        d['구분(new)'] = d['[매출처]'].map(category_map).fillna(category_map.get('default', '기타'))
        if seeding_map:
            seed_cond = (d['[브랜드]'] == seeding_map['brand']) & (d['[매출처]'] == seeding_map['mall'])
            d.loc[seed_cond, '구분(new)'] = seeding_map['category']
        return d

    df_main = assign_category(df_main, config['category_map'], config.get('seeding_map'))
    df_store_abnormal = assign_category(df_store_abnormal, config['category_map'], config.get('seeding_map'))
    
    def finalize(d, cols_map, is_abnormal=False):
        if d.empty: return pd.DataFrame()
        d = d.copy()
        d['자료출처'] = '삼일 출고데이터'
        d['단위'] = 1
        
        rename_map = {v: k for k,v in cols_map.items() if not k.startswith('_')}
        rename_map.update({'구분(new)': '구분(new)', '자료출처': '자료출처', '단위': '단위(EA)'})
        
        final_df = d.rename(columns=rename_map)
        
        col_order = ['일자', '주문번호', '구분(new)', '구분', '상품코드', '품목명', '단위(EA)', '수량', '자료출처']
        if is_abnormal:
            col_order.insert(0, '출고타입')
        
        for col in col_order:
            if col not in final_df.columns: final_df[col] = pd.NA
        
        return final_df[[c for c in col_order if c in final_df.columns]]

    return finalize(df_main, config['final_columns']), \
           finalize(df_shipping_type_abnormal, config['final_columns'], True), \
           finalize(df_store_abnormal, config['final_columns'])

def process_return_data(df):
    config = CONFIG['rules']['return']
    if df.empty:
        return pd.DataFrame()

    # 1) 이전 달 필터
    df = filter_by_previous_month(df, config['date_col'])
    if df.empty:
        return pd.DataFrame()

    # 2) 브랜드 정리 및 제외
    brand_col = config['brand_col']
    df[brand_col] = df[brand_col].astype(str).str.split(':').str[0].str.strip()
    df = df[~df[brand_col].isin(CONFIG['general']['excluded_brands'])].copy()

    # 3) 기본 수량/상태 컬럼 처리
    qty_col = config['qty_col']
    status_col = config['status_col']
    df[qty_col] = pd.to_numeric(df[qty_col], errors='coerce').fillna(0)

    # 4) 기본 행: 모두 '반품'
    df_return = df.copy()
    df_return['구분(new)'] = '반품'

    # 5) 불량/파손 등 상태 매핑된 행: 구분(new) 값을 교체
    df_bad_pason = df[df[status_col].isin(config['bad_pason_map'].keys())].copy()
    if not df_bad_pason.empty:
        df_bad_pason['구분(new)'] = df_bad_pason[status_col].map(config['bad_pason_map'])

    # 6) 합치고 공통 메타 컬럼 추가
    df_final = pd.concat([df_return, df_bad_pason], ignore_index=True)
    df_final['자료출처'] = '삼일 반품데이터'
    df_final['단위'] = 1

    # 7) ★ 수량 부호 규칙 ★
    #    - 기본은 모두 양수(절대값)
    #    - '불량'만 음수로 변환
    df_final[qty_col] = pd.to_numeric(df_final[qty_col], errors='coerce').fillna(0).abs()
    df_final.loc[df_final['구분(new)'] == '반품', qty_col] *= -1
    df_final.loc[df_final['구분(new)'] == '불량', qty_col] *= 1

    # 8) 컬럼 리네임 및 최종 컬럼 순서
    rename_map = {v: k for k, v in config['final_columns'].items() if not k.startswith('_')}
    rename_map.update({'구분(new)': '구분(new)', '자료출처': '자료출처', '단위': '단위(EA)'})
    final_df = df_final.rename(columns=rename_map)

    col_order = ['일자', '주문번호', '구분(new)', '구분', '상품코드', '품목명', '단위(EA)', '수량', '자료출처']
    return final_df[[c for c in col_order if c in final_df.columns]]


def process_receiving_data(df):
    config = CONFIG['rules']['receiving']
    if df.empty: return pd.DataFrame(), pd.DataFrame()
    
    df = filter_by_previous_month(df, config['date_col'])
    if df.empty: return pd.DataFrame(), pd.DataFrame()
    
    df['[브랜드]'] = df['[브랜드]'].astype(str).str.split(':').str[0].str.strip()
    df = df[~df['[브랜드]'].isin(CONFIG['general']['excluded_brands'])].copy()

    df_free = df[df[config['type_col']].isin(config['free_types'])].copy()
    df_peculiar = df[~df[config['type_col']].isin(config['free_types'])].copy()
    
    for col, val in config['peculiar_filters'].items():
        if col.startswith('_'): continue
        if isinstance(val, list):
            df_peculiar = df_peculiar[~df_peculiar[col].isin(val)].copy()
        else:
            df_peculiar = df_peculiar[df_peculiar[col] != val].copy()

    def finalize(d, cols_map, is_free=False):
        if d.empty: return pd.DataFrame()
        d = d.copy()
        d['자료출처'] = '삼일 입고데이터'
        d['단위'] = 1

        # 원본 수량 보관
        qty_src_col = config['qty_col']
        orig_qty = pd.to_numeric(d[qty_src_col], errors='coerce').fillna(0)

        # 리네임: 원본 수량/단위 컬럼은 매핑에서 제외(중복 방지)
        rename_map = {
            v: k for k, v in cols_map.items()
            if not k.startswith('_') and v not in [qty_src_col, '단위']
        }
        final_df = d.rename(columns=rename_map)

        # 단일 수량/단위 컬럼 생성
        if is_free:
            # 무상 정상 입고는 기존 로직 유지(요청이 특이사항만이라 그대로 둡니다)
            final_df['수량'] = orig_qty
            final_df['구분(new)'] = final_df['구분'].str.split(' : ').str[1]
        else:
            # 👉 입고 특이사항: 수량은 원본 절댓값을 음수로
            final_df['수량'] = -orig_qty.abs()
            final_df['구분(new)'] = '반품'

        # 단위(EA)는 항상 1
        final_df['단위(EA)'] = 1
        # 혹시 남아있을 수 있는 '단위' 원본 컬럼 제거
        final_df.drop(columns=['단위'], errors='ignore')

        final_cols = [
            '일자', '주문번호', '구분(new)', '구분', '상품코드', '품목명',
            '단위(EA)', '수량', '자료출처', '상태', '상품비고', '브랜드'
        ]
        for col in final_cols:
            if col not in final_df.columns:
                final_df[col] = pd.NA

        return final_df[final_cols]


    # 반환: (입고 특이사항, 무상 정상 입고)
    return finalize(df_peculiar, config['final_columns_peculiar']), \
           finalize(df_free, config['final_columns_free'], True)


# --- 3. Streamlit UI 구성 ---

st.set_page_config(page_title="월초 정산 프로그램", layout="wide")
st.title("🚀 월초 정산 요약 프로그램 v2.8 (안정화 버전)")
st.markdown("`삼일` 창고의 출고, 반품, 입고 파일들을 한 번에 업로드하면 정산 요약 엑셀 파일을 생성합니다.")

uploaded_files = st.file_uploader(
    "정산할 엑셀 파일들을 한 번에 선택하세요 (출고, 반품, 입고)",
    type=['xlsx', 'xls'],
    accept_multiple_files=True
)

if 'processing_done' not in st.session_state:
    st.session_state.processing_done = False

if st.button("정산 시작! ✨", disabled=(not uploaded_files)):
    st.session_state.processing_done = False
    try:
        df_shipping = pd.DataFrame()
        df_return = pd.DataFrame()
        df_receiving = pd.DataFrame()
        
        shipping_parts = []
        return_parts = []
        receiving_parts = []

        with st.spinner('파일을 읽고 분류하는 중...'):
            for up_file in uploaded_files:
                df_temp = pd.read_excel(up_file)
                if CONFIG['file_identifiers']['shipping'] in df_temp.columns:
                    shipping_parts.append(df_temp)
                    st.info(f"✅ 출고 파일 확인: {up_file.name}")
                elif CONFIG['file_identifiers']['return'] in df_temp.columns:
                    return_parts.append(df_temp)
                    st.info(f"✅ 반품 파일 확인: {up_file.name}")
                elif CONFIG['file_identifiers']['receiving'] in df_temp.columns:
                    receiving_parts.append(df_temp)
                    st.info(f"✅ 입고 파일 확인: {up_file.name}")
                else:
                    st.warning(f"⚠️ 파일 유형 식별 불가: {up_file.name}")
        
        if shipping_parts: df_shipping = pd.concat(shipping_parts, ignore_index=True)
        if return_parts: df_return = pd.concat(return_parts, ignore_index=True)
        if receiving_parts: df_receiving = pd.concat(receiving_parts, ignore_index=True)

        with st.spinner('데이터를 처리하고 엑셀 파일을 생성하는 중...'):
            main_shipping, type_abnormal, store_abnormal = process_shipping_data(df_shipping)
            main_return = process_return_data(df_return)
            peculiar_receiving, free_receiving = process_receiving_data(df_receiving)

            df_summary_final = pd.concat([main_shipping, main_return], ignore_index=True)
            
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                # ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
                # 날짜 형식 변환 로직 추가
                # ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
                def format_date_columns(df_to_format):
                    if '일자' in df_to_format.columns:
                        # NaT 값을 무시하고 날짜 형식으로 변환
                        df_to_format['일자'] = pd.to_datetime(df_to_format['일자'], errors='coerce').dt.strftime('%Y-%m-%d')
                    return df_to_format

                # 각 데이터프레임을 엑셀에 쓰기 전에 날짜 형식 변환
                if not df_summary_final.empty: format_date_columns(df_summary_final).to_excel(writer, sheet_name='정산요약', index=False)
                if not store_abnormal.empty: format_date_columns(store_abnormal).to_excel(writer, sheet_name='매출처 이상', index=False)
                if not type_abnormal.empty: format_date_columns(type_abnormal).to_excel(writer, sheet_name='출고타입 이상', index=False)
                if not peculiar_receiving.empty: format_date_columns(peculiar_receiving).to_excel(writer, sheet_name='입고 특이사항', index=False)
                if not free_receiving.empty: format_date_columns(free_receiving).to_excel(writer, sheet_name='무상 정상 입고', index=False)
                
                total_processed = len(main_shipping) + len(type_abnormal) + len(store_abnormal)
                validation_data = {
                    '항목': ['일반 출고', '출고타입 이상', '매출처 이상', '처리된 총 출고건수 (검증용)'],
                    '건수': [len(main_shipping), len(type_abnormal), len(store_abnormal), total_processed]
                }
                pd.DataFrame(validation_data).to_excel(writer, sheet_name='검증', index=False)
            
            st.session_state.excel_output = output.getvalue()
            st.session_state.processing_done = True

    except Exception as e:
        st.error("처리 중 오류가 발생했습니다. 아래 메시지를 확인해 주세요.")
        st.code(f"""
        에러 타입: {type(e).__name__}
        에러 메시지: {e}
        ---
        상세 정보:
        {traceback.format_exc()}
        """)
        st.session_state.processing_done = False

if st.session_state.processing_done:
    st.balloons()
    st.header("🎉 정산 완료! 아래에서 결과 엑셀 파일을 다운로드하세요.")
    
    now = datetime.now().strftime('%y%m%d_%H%M')
    st.download_button(
        label="📥 최종 엑셀 파일 다운로드",
        data=st.session_state.excel_output,
        file_name=f"정산요약_{now}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
