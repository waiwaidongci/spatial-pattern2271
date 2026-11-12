import {RunStatus} from '../../constants/enums';
export function StatusBadge({status,attempts=0}:{status:string;attempts?:number}){
  const recovered = attempts>1 && (status==='running'||status==='queued');
  return <span className={'status '+status} title={recovered?`中断后已自动接手，第 ${attempts} 次执行`:undefined}><i/>{RunStatus[status as keyof typeof RunStatus]||status}{recovered?` · 已接手第${attempts}次执行`:''}</span>;
}
