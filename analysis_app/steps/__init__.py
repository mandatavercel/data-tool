"""
analysis_app.steps — 6단계 워크플로우 각 step의 render() 함수.

메인 router는 step 번호 → render 함수 매핑 dict로 dispatch:

    from analysis_app.steps import step1_upload, step2_schema, ...
    STEP_RENDERS = {1: step1_upload.render, 2: step2_schema.render, ...}
    STEP_RENDERS[current_step]()
"""
