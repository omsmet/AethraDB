package AethraDB.util.JNI;

import org.graalvm.nativeimage.c.CContext;
import org.graalvm.nativeimage.c.function.CFunctionPointer;
import org.graalvm.nativeimage.c.function.InvokeCFunctionPointer;
import org.graalvm.nativeimage.c.struct.CField;
import org.graalvm.nativeimage.c.struct.CStruct;
import org.graalvm.nativeimage.c.type.CCharPointer;
import org.graalvm.word.PointerBase;

@CContext(JNIHeaderDirectives.class)
@CStruct(value = "JNIEnv_", addStructKeyword = true)
public interface JNIEnv extends PointerBase {

    @CField("functions")
    JNINativeInterface getFunctions();

    @CStruct(value = "JNINativeInterface_", addStructKeyword = true)
    interface JNINativeInterface extends PointerBase {
        @CField
        NewStringUTF getNewStringUTF();

        @CField
        GetStringUTFChars getGetStringUTFChars();
    }

    interface JObject extends PointerBase {
    }

    interface JString extends JObject {
    }

    interface NewStringUTF extends CFunctionPointer {
        @InvokeCFunctionPointer
        JString call(JNIEnv env, CCharPointer cCharPointer);
    }

    interface GetStringUTFChars extends CFunctionPointer {
        @InvokeCFunctionPointer
        CCharPointer call(JNIEnv env, JString str, byte isCopy);
    }
}