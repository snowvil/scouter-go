// Scouter wire format: Copyright 2015 the original author or authors,
// https://github.com/scouter-project/scouter, Apache License 2.0.
// Synthetic fixtures only. Real Scouter StepEnum/DataOutputX/DataInputX are used.
import java.io.*;
import java.lang.reflect.*;
import java.nio.file.*;
import java.util.*;
import scouter.io.*;
import scouter.lang.step.*;
import scouter.lang.value.*;

public final class StepOracle {
    private static Path output;
    private static final Map<String,Integer> integers=new HashMap<>();
    static {
        integers.put("parent",-7); integers.put("index",17);
        integers.put("start_time",128); integers.put("start_cpu",127);
        integers.put("hash",-1234567); integers.put("elapsed",83); integers.put("cputime",84);
        integers.put("error",-32769); integers.put("time",85); integers.put("value",86);
        integers.put("count",3); integers.put("port",33000); integers.put("updated",-2);
        integers.put("code",99); integers.put("localEndpointServiceName",101); integers.put("remoteEndpointServiceName",102);
    }
    private static Value list(String name) {
        ListValue list=new ListValue();
        if(name.equals("annotationTimestamps")) {list.add(1700000000123L);list.add(1700000000999L);}
        else {list.add("synthetic-start");list.add("synthetic-end");}
        return list;
    }
    private static Step sample(StepEnum.Type type,boolean zero) throws Exception {
        Step step=type.create();
        for(Field field:step.getClass().getFields()) {
            if(Modifier.isStatic(field.getModifiers()))continue;
            String name=field.getName();Class<?> kind=field.getType();
            if(kind==ListValue.class) {field.set(step,zero?new ListValue():list(name));continue;}
            if(kind==MapValue.class) {
                MapValue tags=new MapValue();if(!zero){tags.put("z",new DecimalValue(9));tags.put("a",new TextValue("synthetic-tag"));}
                field.set(step,tags);continue;
            }
            if(zero)continue;
            if(kind==int.class)field.setInt(step,integers.getOrDefault(name,44));
            else if(kind==short.class)field.setShort(step,(short)33000);
            else if(kind==long.class) {
                long v=name.equals("timestamp")?1700000000123L:
                    name.equals("elapsed")?549755813887L:
                    name.equals("cputime")?Long.MAX_VALUE-7:
                    name.equals("lockOwnerId")?Long.MAX_VALUE-9:Long.MIN_VALUE+7;
                field.setLong(step,v);
            } else if(kind==byte.class)field.setByte(step,(byte)(name.equals("opt") || name.equals("threaded")?1:249));
            else if(kind==boolean.class)field.setBoolean(step,true);
            else if(kind==String.class)field.set(step,"synthetic-"+name+" 한글");
            else if(kind==byte[].class)field.set(step,new byte[]{(byte)192,0,2,7});
            else if(kind==int[].class)field.set(step,new int[]{Integer.MIN_VALUE,-7,0,Integer.MAX_VALUE});
        }
        if(step instanceof ApiCallSum)((ApiCallSum)step).opt=0;
        if(step instanceof ParameterizedMessageStep) {
            ParameterizedMessageStep p=(ParameterizedMessageStep)step;
            if(!zero){p.setMessage(-1234567,"synthetic","한글");p.setElapsed(83);p.setLevelOfByte((byte)-7);}
        }
        // The array writer's filter is producer behavior, not a wire field.
        if(step instanceof ThreadCallPossibleStep)((ThreadCallPossibleStep)step).isIgnoreIfNoThreaded=false;
        return step;
    }
    private static void equalValues(Value a,Value b) throws Exception {
        if(a.getValueType()!=b.getValueType())throw new AssertionError("Value type mismatch");
        if(a instanceof MapValue) {
            Map<String,Value>x=((MapValue)a).toMap(),y=((MapValue)b).toMap();
            if(!x.keySet().equals(y.keySet()))throw new AssertionError("Value map keys");
            for(String key:x.keySet())equalValues(x.get(key),y.get(key));
        } else if(a instanceof ListValue) {
            ListValue x=(ListValue)a,y=(ListValue)b;
            if(x.size()!=y.size())throw new AssertionError("Value list size");
            for(int i=0;i<x.size();i++)equalValues(x.get(i),y.get(i));
        } else if(!Arrays.equals(new DataOutputX().writeValue(a).toByteArray(),new DataOutputX().writeValue(b).toByteArray()))throw new AssertionError("Value semantics");
    }
    private static void assertSingle(Step a,Step b) throws Exception {
        if(a.getStepType()!=b.getStepType())throw new AssertionError("Step ID");
        for(Field field:a.getClass().getFields()) {
            String name=field.getName();
            if(Modifier.isStatic(field.getModifiers()) || name.equals("spanPack") || name.equals("start") || name.equals("nameTemp") || name.equals("nameDebug") || name.equals("isIgnoreIfNoThreaded"))continue;
            Object x=field.get(a),y=field.get(b);
            if(x instanceof Value){equalValues((Value)x,(Value)y);continue;}
            // null text/blob/array has the same wire representation as empty.
            if(x==null && (y==null || y instanceof String && ((String)y).isEmpty() || y instanceof byte[] && ((byte[])y).length==0 || y instanceof int[] && ((int[])y).length==0))continue;
            if(!Objects.deepEquals(x,y))throw new AssertionError(a.getClass().getName()+" field "+name+" differs");
        }
        if(a instanceof ParameterizedMessageStep) {
            ParameterizedMessageStep x=(ParameterizedMessageStep)a,y=(ParameterizedMessageStep)b;
            // The real writer exposes private level/paramString through encoded bytes.
            if(x.getHash()!=y.getHash() || x.getElapsed()!=y.getElapsed() || !Arrays.equals(new DataOutputX().writeStep(x).toByteArray(),new DataOutputX().writeStep(y).toByteArray()))throw new AssertionError("parameterized private fields");
        }
    }
    private static byte[] emit(String name,Step step) throws Exception {
        byte[] bytes=new DataOutputX().writeStep(step).toByteArray();
        DataInputX in=new DataInputX(bytes);Step decoded=in.readStep();assertSingle(step,decoded);
        if(in.available()!=0)throw new AssertionError("trailing Java Step data");
        Files.write(output.resolve("step-"+name+".bin"),bytes);
        return bytes;
    }
    public static void main(String[] args) throws Exception {
        output=Paths.get(args[0]);Files.createDirectories(output);
        List<Step> sequence=new ArrayList<>();
        for(StepEnum.Type type:StepEnum.Type.values()) {
            Step step=sample(type,false);emit(type.name(),step);emit(type.name()+"-zero",sample(type,true));sequence.add(step);
        }
        for(StepEnum.Type type:new StepEnum.Type[]{StepEnum.Type.APICALL,StepEnum.Type.APICALL2,StepEnum.Type.DISPATCH,StepEnum.Type.SPANCALL}) {
            for(int opt:new int[]{0,255}) {
                Step step=sample(type,false);step.getClass().getField("opt").setByte(step,(byte)opt);
                // address is only serialized for opt=1.
                step.getClass().getField("address").set(step,null);
                emit(type.name()+"-opt"+opt,step);
            }
        }
        Files.write(output.resolve("step-sequence.bin"),Step.toBytes(sequence));
        Collections.reverse(sequence);Files.write(output.resolve("step-sequence-reversed.bin"),Step.toBytes(sequence));
        Files.write(output.resolve("step-empty.bin"),Step.toBytes(Collections.<Step>emptyList()));
        // Verify the EOF sequence reader on real Java, preserving input order.
        List<Step> decoded=Step.toObjectList(Files.readAllBytes(output.resolve("step-sequence-reversed.bin")));
        if(decoded.size()!=sequence.size())throw new AssertionError("EOF Step sequence size");
        for(int i=0;i<sequence.size();i++)assertSingle(sequence.get(i),decoded.get(i));
        ApiCallSum invalid=new ApiCallSum();invalid.opt=1;
        try {new DataOutputX().writeStep(invalid);throw new AssertionError("Java API sum opt=1 unexpectedly allowed");}
        catch(IOException expected) { /* supported writer has only opt=0 */ }
    }
}
