# منسق مخازن الحبوب والزراعات الذكية السحابية والتراجع التلقائي

المسار المطلوب:

```text
/src/services/EtlOrchestrator.ts
```

## ماذا ينفّذ المشروع؟

ينفّذ نظام **Grain Silo Processing Coordinator** و **Agriculture Smart Sensors Orchestrator** وفق المواصفات:

- DAG خطي: `Extract -> Transform -> Load -> Aggregate`
- معالجة بيانات MapReduce مبسطة حسب المخزن `siloId`
- تحقق من بيانات الحساسات قبل التحويل
- تخزين سحابي محاكى للبيانات الخام والمعالجة والتجميع
- Saga Orchestrator للتراجع التلقائي عند فشل أي خطوة
- Compensating Transactions لتنظيف التخزين عند الفشل
- Dynamic status feed لكل مرحلة
- قياس latency لكل مرحلة وتحديد أبطأ خطوة Bottleneck

## التشغيل

```bash
npm install
npm run start
```

أو:

```bash
npm run start:compiled
```

## سيناريوهات الاختبار

البرنامج يشغّل حالتين تلقائياً:

1. تشغيل ناجح لكل مراحل ETL.
2. فشل متعمد في مرحلة `aggregate-storage` لإثبات التراجع التلقائي.

## ربط المتطلبات بالكود

| المتطلب | مكان التنفيذ |
|---|---|
| Extract/Transform/Load/Aggregate status feeds | `statusFeed` داخل `PipelineContext` |
| DAG dependencies | `dependsOn` داخل كل `PipelineStep` |
| validation across sharded clusters | `validateAndTransform()` و `shardId` |
| rollback states across nodes | `rollback()` و `compensate()` |
| latency tracking | `latenciesMs` و `findBottleneck()` |
| storage compensations | `SimulatedCloudStorage.delete()` |
| Saga model | كل خطوة تحتوي `execute` و `compensate` |

## شرح مختصر أكاديمي

بما أن معاملات ACID التقليدية لا تعمل بسهولة عبر مخازن سحابية منفصلة، يستخدم المشروع نمط **Saga**. كل خطوة ناجحة تسجّل إجراءً تعويضياً. إذا فشلت خطوة لاحقة، ينفّذ المنسق إجراءات التعويض بالترتيب العكسي، مثل حذف السجلات التي تم تحميلها أو تنظيف التجميعات الجزئية.
